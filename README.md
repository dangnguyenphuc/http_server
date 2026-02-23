# Simple HTTP/1.1 server

Implementation of a simple HTTP server in C++ by liburing (io_uring)

## Features

- Can handle multiple concurrent connections, tested up to 10k.
- Support basic HTTP request and response. Provide an extensible framework to implement other HTTP features.
- HTTP/1.1: Persistent connection is enabled by default.

## Quick start

```bash
mkdir build && cd build
cmake ..
make
./test_SimpleHttpServer # Run unit tests
./SimpleHttpServer      # Start the HTTP server on port 8080
```

- There are two endpoints available at `/` and `/hello.html` which are created for demo purpose.
- In order to have multiple concurrent connections, make sure to raise the resource limit (with `ulimit`) before running the server. A non-root user by default can have about 1000 file descriptors opened, which corresponds to 1000 active clients.


## Benchmark

- Epoll implementation:

```bash
$ ./wrk -t10 -c500 -d60s http://0.0.0.0:8080/
Running 1m test @ http://0.0.0.0:8080/
  10 threads and 500 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     5.01ms    1.31ms  57.86ms   86.35%
    Req/Sec     9.94k     0.99k   36.28k    76.69%
  5933266 requests in 1.00m, 441.36MB read
Requests/sec:  98760.82
Transfer/sec:      7.35MB
```

```bash
$ ./wrk -t10 -c10000 -d60s http://0.0.0.0:8080/
Running 1m test @ http://0.0.0.0:8080/
  10 threads and 10000 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   111.78ms   21.38ms 403.80ms   76.79%
    Req/Sec     8.73k     1.42k   18.77k    75.62%
  5174508 requests in 1.00m, 384.91MB read
Requests/sec:  86123.84
Transfer/sec:      6.41MB

```

## Need to improve

- So currently my code cannot run even with -c500
- Safe pattern:

```cpp
while (running) {

    // Try to submit as much as possible
    int ret = io_uring_submit(&ring);
    if (ret < 0) {
        perror("submit");
        break;
    }

    // Block until at least 1 completion
    io_uring_cqe* cqe;
    ret = io_uring_wait_cqe(&ring, &cqe);
    if (ret < 0) {
        perror("wait_cqe");
        break;
    }

    // Drain all completions
    do {
        handle_completion(cqe);
        io_uring_cqe_seen(&ring, cqe);
    } while (!io_uring_peek_cqe(&ring, &cqe));
}
```