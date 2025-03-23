# MapReduce in Go

This project implements a **MapReduce framework** in Go, including:
- A **sequential** MapReduce library supporting arbitrary map and reduce functions.
- A **word count application** using the MapReduce paradigm.
- A **fault-tolerant distributed system** that handles worker failures.

## 🚀 Features
- **Sequential MapReduce execution**
- **Distributed execution with fault tolerance**
- **Word count example using MapReduce**
- **Worker failure handling**

## 📂 Project Structure
```
mapreduce/       # Core MapReduce library
  ├── map_reduce.go      # Sequential MapReduce implementation
  ├── master.go          # Master node logic
  ├── schedule.go        # Task scheduling for distributed mode
  ├── worker.go          # Worker logic for distributed mode

main/            # Example MapReduce application
  ├── word_count.go      # Word count implementation

papers/          # Input dataset (most cited security papers)
```

## 🛠️ Setup & Run
### 1️⃣ Install Go
Ensure you have Go installed. Download it from [golang.org](https://go.dev/).

### 2️⃣ Clone the Repository
```sh
git clone https://github.com/YOUR_USERNAME/mapreduce-go.git
cd mapreduce-go
```

### 3️⃣ Run Word Count Example
#### **Sequential Execution**
```sh
go run main/word_count.go master sequential papers
```

#### **Distributed Execution**
1. Start workers in separate terminals:
```sh
go run worker.go
```
2. Run the master node:
```sh
go run main/word_count.go master distributed papers
```

## ✅ Testing
Run unit tests to validate the implementation:
```sh
go test -run TestSequentialSingle mapreduce/...
go test -run TestSequentialMany mapreduce/...
go test -run TestBasic mapreduce/...
```

## 📜 Acknowledgments
This project is adapted from assignments used in Princeton’s COS418 and MIT 6.824 distributed systems courses.

## 📌 License
MIT License.
