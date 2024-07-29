# Data loader: Csf / Cdf CSV

This small POC program is to show we can load data quickly into Redis and query Redis with the email hash.

The `loaderv1` uses standard Go libraries to load data into Redis. 
The `loader` uses `github.com/edsrzf/mmap-go`


## How to run the program (loading data into Redis)

1. Clone the repository
2. Have a redis instance running on the default port
3. Run the following command to install the dependencies
```bash
go run ./cmd/loader -file=path_to_csv

# If you want to flush database first
go run ./cmd/loader -flush=true -file=path_to_csv
```

```bash
go run ./cmd/loaderv1 -file=path_to_csv

# If you want to flush database first
go run ./cmd/loaderv1 -flush=true -file=path_to_csv
```


## How to run the program (querying Redis)
```bash
go run ./cmd/api
```

### API Endpoints
1. GET `http://localhost:8099/search?q=emailHash`
