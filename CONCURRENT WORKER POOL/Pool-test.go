package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Table-driven parser cases
// ---------------------------------------------------------------------------

type kind int

const (
	kindOK kind = iota
	kindInvalid
	kindDecode
)

var cases = []struct {
	name  string
	input string
	want  kind
}{
	{"good", `{"id":"s-1","ts":1700000000000,"temp":22.5,"hum":41.0}`, kindOK},
	{"extra fields", `{"id":"s-1","ts":1700000000000,"temp":22.5,"hum":41.0,"battery":88,"fw":"1.2"}`, kindOK},
	{"unicode id", `{"id":"sensor-é-7","ts":1,"temp":0,"hum":0}`, kindOK},
	{"truncated", `{"id":`, kindDecode},
	{"empty id", `{"id":"","ts":1,"temp":1,"hum":1}`, kindInvalid},
	{"zero ts", `{"id":"s-2","ts":0,"temp":1,"hum":1}`, kindInvalid},
	{"negative ts", `{"id":"s-2","ts":-5,"temp":1,"hum":1}`, kindInvalid},
	{"missing ts", `{"id":"s-2","temp":1,"hum":1}`, kindInvalid},
	{"empty input", ``, kindDecode},
	{"plain text", `hello`, kindDecode},
	{"ts wrong type", `{"id":"s-3","ts":"abc","temp":1,"hum":1}`, kindDecode},
	{"json null", `null`, kindInvalid},
	{"json array", `[]`, kindDecode},
}

func TestParseJSON(t *testing.T) {
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := ParseJSON([]byte(c.input))
			switch c.want {
			case kindOK:
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if r.SensorID == "" {
					t.Fatal("SensorID is empty")
				}
			case kindInvalid:
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("want ErrInvalid, got %v", err)
				}
			case kindDecode:
				if err == nil || errors.Is(err, ErrInvalid) {
					t.Fatalf("want a decode error, got %v", err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Pool tests
// ---------------------------------------------------------------------------

// Pushes every table case through the pool with tiny buffers, so Submit has
// to apply backpressure, then checks the counts and the Stats snapshot.
func TestPoolEndToEnd(t *testing.T) {
	p := NewPool(Config{Workers: 4, QueueSize: 4, ResultSize: 4})
	p.Start(context.Background())

	var okCount, badCount int
	done := make(chan struct{})
	go func() {
		for r := range p.Results() {
			if r.Err != nil {
				badCount++
			} else {
				okCount++
			}
		}
		close(done)
	}()

	wantOK := 0
	for _, c := range cases {
		if c.want == kindOK {
			wantOK++
		}
		if err := p.Submit(context.Background(), []byte(c.input)); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out: results channel was never closed")
	}

	wantBad := len(cases) - wantOK
	if okCount != wantOK || badCount != wantBad {
		t.Fatalf("ok=%d bad=%d, want ok=%d bad=%d", okCount, badCount, wantOK, wantBad)
	}
	s := p.Stats()
	if s.Parsed != uint64(wantOK) || s.Failed != uint64(wantBad) {
		t.Fatalf("stats mismatch: %+v", s)
	}
}

// 1000 payloads through 16-slot buffers; Close must drain everything and
// Results must close on its own.
func TestPoolParsesAndDrainsOnClose(t *testing.T) {
	p := NewPool(Config{Workers: 4, QueueSize: 16, ResultSize: 16})
	p.Start(context.Background())

	const n = 1000
	done := make(chan struct{})
	var ok, bad int
	go func() {
		for r := range p.Results() {
			if r.Err != nil {
				bad++
			} else {
				ok++
			}
		}
		close(done)
	}()

	for i := 0; i < n; i++ {
		payload := []byte(fmt.Sprintf(`{"id":"s-%d","ts":1,"temp":1,"hum":1}`, i))
		if i%10 == 0 {
			payload = []byte(`not json`)
		}
		if err := p.Submit(context.Background(), payload); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for drain")
	}
	if ok != 900 || bad != 100 {
		t.Fatalf("ok=%d bad=%d", ok, bad)
	}
}

func TestSubmitAfterClose(t *testing.T) {
	p := NewPool(Config{Workers: 1})
	p.Start(context.Background())
	p.Close()
	if err := p.Submit(context.Background(), []byte("x")); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("got %v, want ErrPoolClosed", err)
	}
	for range p.Results() { // drain until closed
	}
}

func TestTrySubmitDropsWhenFull(t *testing.T) {
	// Not started: nothing drains the queue, so it fills deterministically.
	p := NewPool(Config{Workers: 1, QueueSize: 2})
	_ = p.TrySubmit([]byte("a"))
	_ = p.TrySubmit([]byte("b"))
	if err := p.TrySubmit([]byte("c")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("got %v, want ErrQueueFull", err)
	}
	if p.Stats().Dropped != 1 {
		t.Fatalf("dropped = %d, want 1", p.Stats().Dropped)
	}
}

func TestParserPanicDoesNotKillWorker(t *testing.T) {
	p := NewPool(Config{Workers: 1, Parse: func(b []byte) (Reading, error) {
		if string(b) == "boom" {
			panic("bad parser")
		}
		return Reading{SensorID: "ok", Timestamp: 1}, nil
	}})
	p.Start(context.Background())
	_ = p.Submit(context.Background(), []byte("boom"))
	_ = p.Submit(context.Background(), []byte("fine"))
	p.Close()

	var errs, oks int
	for r := range p.Results() {
		if r.Err != nil {
			errs++
		} else {
			oks++
		}
	}
	if errs != 1 || oks != 1 {
		t.Fatalf("errs=%d oks=%d", errs, oks)
	}
}
