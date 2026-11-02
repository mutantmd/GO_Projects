package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Reading struct {
	SensorID    string  `json:"id"`
	Timestamp   int64   `json:"ts"`
	Temperature float64 `json:"temp"`
	Humidity    float64 `json:"hum"`
}

type Result struct {
	Reading Reading
	Err     error
	Latency time.Duration
}

var ErrInvalid = errors.New("invalid reading")

type ParseFunc func([]byte) (Reading, error)

func ParseJSON(b []byte) (Reading, error) {
	var r Reading
	if err := json.Unmarshal(b, &r); err != nil {
		return Reading{}, fmt.Errorf("decode: %w", err)
	}
	if r.SensorID == "" || r.Timestamp <= 0 {
		return Reading{}, ErrInvalid
	}
	return r, nil
}

var (
	ErrPoolClosed = errors.New("pool closed")
	ErrQueueFull  = errors.New("queue full")
)

type Config struct {
	Workers    int
	QueueSize  int
	ResultSize int
	Parse      ParseFunc
}

type job struct {
	payload    []byte
	enqueuedAt time.Time
}

// Stats is a snapshot of the pool's counters.
type Stats struct {
	Submitted uint64
	Parsed    uint64
	Failed    uint64
	Dropped   uint64
	Queued    int
}

type Pool struct {
	cfg     Config
	jobs    chan job
	results chan Result
	wg      sync.WaitGroup

	mu     sync.RWMutex
	closed bool

	submitted, parsed, failed, dropped atomic.Uint64
}

func NewPool(cfg Config) *Pool {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1024
	}
	if cfg.ResultSize <= 0 {
		cfg.ResultSize = 1024
	}
	if cfg.Parse == nil {
		cfg.Parse = ParseJSON
	}
	return &Pool{
		cfg:     cfg,
		jobs:    make(chan job, cfg.QueueSize),
		results: make(chan Result, cfg.ResultSize),
	}
}


func (p *Pool) Start(ctx context.Context) {
	p.wg.Add(p.cfg.Workers)
	for i := 0; i < p.cfg.Workers; i++ {
		go p.worker(ctx)
	}

	go func() {
		p.wg.Wait()
		close(p.results)
	}()
}

// Results is the read-only output stream.
func (p *Pool) Results() <-chan Result { return p.results }


func (p *Pool) Submit(ctx context.Context, payload []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrPoolClosed
	}
	select {
	case p.jobs <- job{payload: payload, enqueuedAt: time.Now()}:
		p.submitted.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TrySubmit never blocks. If the queue is full the payload is dropped and counted.
func (p *Pool) TrySubmit(payload []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrPoolClosed
	}
	select {
	case p.jobs <- job{payload: payload, enqueuedAt: time.Now()}:
		p.submitted.Add(1)
		return nil
	default:
		p.dropped.Add(1)
		return ErrQueueFull
	}
}

// Close stops accepting work and lets workers drain what is already queued.
// Safe to call more than once.
func (p *Pool) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.jobs)
	}
	p.mu.Unlock()
}

// Wait blocks until all workers have exited.
func (p *Pool) Wait() { p.wg.Wait() }

func (p *Pool) Stats() Stats {
	return Stats{
		Submitted: p.submitted.Load(),
		Parsed:    p.parsed.Load(),
		Failed:    p.failed.Load(),
		Dropped:   p.dropped.Load(),
		Queued:    len(p.jobs),
	}
}

func (p *Pool) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case j, ok := <-p.jobs:
			if !ok {
				return // Close() was called and the queue is drained
			}
			res := p.process(j)
			select {
			case p.results <- res:
			case <-ctx.Done():
				return
			}
		}
	}
}

// process parses one job. A panicking parser fails that one payload
// instead of killing the worker (and the process).
func (p *Pool) process(j job) (res Result) {
	defer func() {
		if r := recover(); r != nil {
			res = Result{Err: fmt.Errorf("parser panic: %v", r)}
		}
		res.Latency = time.Since(j.enqueuedAt)
		if res.Err != nil {
			p.failed.Add(1)
		} else {
			p.parsed.Add(1)
		}
	}()
	r, err := p.cfg.Parse(j.payload)
	return Result{Reading: r, Err: err}
}

// ---------------------------------------------------------------------------
// Demo: producers -> pool -> consumer
// ---------------------------------------------------------------------------

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool := NewPool(Config{
		Workers:    runtime.GOMAXPROCS(0),
		QueueSize:  4096,
		ResultSize: 4096,
	})
	pool.Start(ctx)

	// Consumer: must keep draining Results() or workers will block.
	var consumerDone sync.WaitGroup
	consumerDone.Add(1)
	var (
		okCount, errCount int
		maxLatency        time.Duration
		perSensor         = make(map[string]int)
	)
	go func() {
		defer consumerDone.Done()
		for res := range pool.Results() {
			if res.Err != nil {
				errCount++
				continue
			}
			okCount++
			perSensor[res.Reading.SensorID]++
			if res.Latency > maxLatency {
				maxLatency = res.Latency
			}
		}
	}()

	// Producers: simulate sensors, ~5% malformed payloads.
	runFor := 3 * time.Second
	prodCtx, cancel := context.WithTimeout(ctx, runFor)
	defer cancel()

	var producers sync.WaitGroup
	for p := 0; p < 4; p++ {
		producers.Add(1)
		go func(id int) {
			defer producers.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for prodCtx.Err() == nil {
				var payload []byte
				if rng.Intn(20) == 0 {
					payload = []byte(`{"id":`) // truncated
				} else {
					payload = []byte(fmt.Sprintf(
						`{"id":"s-%d","ts":%d,"temp":%.2f,"hum":%.2f}`,
						rng.Intn(50), time.Now().UnixMilli(),
						15+rng.Float64()*20, 30+rng.Float64()*40))
				}
				_ = pool.TrySubmit(payload) // drop on overload; use Submit for backpressure
			}
		}(p)
	}


	ticker := time.NewTicker(time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s := pool.Stats()
				fmt.Printf("submitted=%d parsed=%d failed=%d dropped=%d queued=%d\n",
					s.Submitted, s.Parsed, s.Failed, s.Dropped, s.Queued)
			case <-prodCtx.Done():
				return
			}
		}
	}()


	producers.Wait()
	pool.Close()
	pool.Wait()
	consumerDone.Wait()

	s := pool.Stats()
	fmt.Println("---- final ----")
	fmt.Printf("ok=%d err=%d sensors=%d max_latency=%s\n", okCount, errCount, len(perSensor), maxLatency)
	fmt.Printf("submitted=%d parsed=%d failed=%d dropped=%d\n", s.Submitted, s.Parsed, s.Failed, s.Dropped)
	fmt.Printf("throughput ≈ %.0f msg/s\n", float64(s.Parsed+s.Failed)/runFor.Seconds())
}