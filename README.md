# 🐹 Go Projects Portfolio

Welcome to my Go (Golang) monorepo! This repository serves as a showcase for practical implementations in Go, ranging from concurrent system designs to interactive backend applications.

---

## 📂 Repository Contents

| Project | Description | Key Concepts / Modules |
| :--- | :--- | :--- |
| [**BOOKING-APP**](./BOOKING-APP) | A CLI-based interactive event/ticket booking system. | Data validation, slices/maps, user input parsing, basic concurrency for ticket generation. |
| [**CONCURRENT WORKER POOL**](./CONCURRENT-WORKER-POOL) | A high-performance background job processing system. | Goroutines, Channels, `sync.WaitGroup`, worker-pool pattern, task queue management. |

---

## 🛠️ Tech Stack & Prerequisites

* **Language:** Go (Golang 1.18+)
* **IDE/Tools:** GoLand / VS Code

Ensure you have Go installed on your machine:
```bash
go version
```

---

## 🏃 Running the Projects

Navigate into the directory of any project and run the main entry file:

### 1. Booking App
```bash
cd BOOKING-APP
go run main.go
```

### 2. Concurrent Worker Pool
```bash
cd "CONCURRENT WORKER POOL"
go run main.go
```

---

## 🧪 Running Tests

To run unit tests across all projects in this repository:

```bash
go test -v ./...
```

---

## 📝 License

This repository is licensed under the [MIT License](LICENSE).