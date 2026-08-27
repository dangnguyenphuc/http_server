// Defines the HTTP server object with some constants and structs
// useful for request handling and improving performance

#ifndef HTTP_SERVER_H_
#define HTTP_SERVER_H_

#include <liburing.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <atomic>
#include <functional>
#include <map>
#include <string>
#include <thread>
#include <utility>
#include <deque>
#include <mutex>
#include <vector>

#include "http_message.h"
#include "uri.h"

namespace simple_http_server {

// Maximum size of an HTTP message is limited by how much bytes
// we can read or send via socket each time
constexpr size_t kMaxBufferSize = 4096;

enum class EventType {
  ACCEPT,
  NOTIFY,
  READ, 
  WRITE
};

struct EventData {
  EventData() : fd(0), length(0), cursor(0), buffer() {}
  int fd;
  EventType type;
  size_t length;
  size_t cursor;
  char buffer[kMaxBufferSize];

  uint64_t notify_value;
};

// A request handler should expect a request as argument and returns a response
using HttpRequestHandler_t = std::function<HttpResponse(const HttpRequest&)>;

class HttpServer {
 public:
  explicit HttpServer(const std::string& host, std::uint16_t port);
  ~HttpServer() = default;

  HttpServer() = default;
  HttpServer(HttpServer&&) = default;
  HttpServer& operator=(HttpServer&&) = default;

  void Start();
  void Stop();
  void RegisterHttpRequestHandler(const std::string& path, HttpMethod method,
                                  const HttpRequestHandler_t callback) {
    Uri uri(path);
    request_handlers_[uri].insert(std::make_pair(method, std::move(callback)));
  }
  void RegisterHttpRequestHandler(const Uri& uri, HttpMethod method,
                                  const HttpRequestHandler_t callback) {
    request_handlers_[uri].insert(std::make_pair(method, std::move(callback)));
  }

  std::string host() const { return host_; }
  std::uint16_t port() const { return port_; }
  bool running() const { return running_; }

 private:
  static constexpr int kBacklogSize = 1000;
  static constexpr int kQueueDepth = 4096;
  static constexpr int kThreadPoolSize = 5;

  std::string host_;
  std::uint16_t port_;
  int sock_fd_;
  std::atomic<bool> running_;
  std::thread listen_thread_;
  std::thread worker_threads_[kThreadPoolSize];
  struct io_uring worker_ring_[kThreadPoolSize];
  struct io_uring listen_ring_{};
  std::mutex worker_mutex_[kThreadPoolSize];
  std::vector<std::deque<int>> worker_pending_fds_;
  std::vector<int> worker_event_fd_;

  std::map<Uri, std::map<HttpMethod, HttpRequestHandler_t>> request_handlers_;

  void CreateSocket();
  void SetUpRings();
  void Listen();
  void ProcessEvents(int worker_id);

  // Helper to push new async requests to the ring
  void SubmitAccept();
  void DispatchConnection(int worker_id, int fd);
  void SubmitWorkerNotify(int worker_id);
  void SubmitRecvRequest(int worker_id, EventData* data);
  void SubmitSendRequest(int worker_id, EventData* data);
  void HandleNotify(int worker_id);
  void HandleRecv(int worker_id, int res, EventData* data);
  void HandleSend(int worker_id, int res, EventData* data);

  void HandleURingEvent(int worker_id, int res, EventData* data);
  // Returns false if it already closed/deleted `request` (e.g. oversized
  // response) — the caller must not touch it again in that case.
  bool HandleHttpData(EventData* request);
  HttpResponse HandleHttpRequest(const HttpRequest& request);

  struct io_uring_sqe* GetWorkerIouringSqe(int worker_id, EventData* data);
  struct io_uring_sqe* GetListenIouringSqe(EventData* data);
};

}  // namespace simple_http_server

#endif  // HTTP_SERVER_H_
