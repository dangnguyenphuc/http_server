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
constexpr size_t kMaxBufferPoolSize = 512;

enum class EventType {
  ACCEPT,
  NOTIFY,
  RECV,
  SEND
};

struct EventDataBase {
  explicit EventDataBase(EventType t) : type(t) {}
  virtual ~EventDataBase() = default;
  EventType type;
};

struct AcceptEvent : EventDataBase {
  AcceptEvent() : EventDataBase(EventType::ACCEPT) {}
};

struct NotifyEvent : EventDataBase {
  NotifyEvent() : EventDataBase(EventType::NOTIFY), notify_value(0) {}
  uint64_t notify_value;
};

struct RecvEvent : EventDataBase {
  RecvEvent() : EventDataBase(EventType::RECV), fd(0), cancelled(false),
      recv_active(true), pending_buffer_id(0), pending_len(0) {}
  int fd;
  bool cancelled;
  bool recv_active;
  // Set once a complete response is ready but the recv hasn't yet been
  // confirmed retired; consumed (pending_len reset implicitly by sending)
  // once `!more` is finally observed.
  uint16_t pending_buffer_id;
  int pending_len;
};

struct SendEvent : EventDataBase {
  SendEvent() : EventDataBase(EventType::SEND), fd(0), length(0), cursor(0), buffer_id(0) {}
  int fd;
  size_t length;
  size_t cursor;
  uint16_t buffer_id;
};

struct ProvidedBuffer {
  char data[kMaxBufferSize];
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
  std::vector<std::vector<ProvidedBuffer>> worker_buffer_pool_;

  std::map<Uri, std::map<HttpMethod, HttpRequestHandler_t>> request_handlers_;

  void CreateSocket();
  void SetUpRings();
  void Listen();
  void ProcessEvents(int worker_id);

  // Helper to push new async requests to the ring
  void SubmitAcceptMultishot();
  void DispatchConnection(int worker_id, int fd);
  void SubmitWorkerNotify(int worker_id);
  void SubmitRecvRequestMultishot(int worker_id, int client_fd);
  void SubmitSendRequest(int worker_id, uint16_t buffer_id, int len, int client_fd);
  void SubmitRemainSendRequest(int worker_id, SendEvent* data);
  void SubmitCancelRecv(int worker_id, RecvEvent* data);
  void HandleNotify(int worker_id);
  void HandleRecv(int worker_id, int res, RecvEvent* data, uint32_t cqe_flags);
  void HandleSend(int worker_id, int res, SendEvent* data);

  void HandleURingEvent(int worker_id, EventDataBase* event, int res, uint32_t cqe_flags);
  // Builds the response in place into `buffer` and returns its length, or 0
  // if the response was too large to fit (the caller must close/free then).
  int HandleHttpData(char* buffer);
  HttpResponse HandleHttpRequest(const HttpRequest& request);

  // A still-armed multishot recv holds its own kernel-side reference to the
  // socket, so close() alone won't make the peer see it close, nor free
  // that reference. shutdown() forces the connection down immediately
  // regardless of that reference; the explicit ASYNC_CANCEL retires the
  // recv's kernel-side tracking right away too, instead of leaving it to
  // shutdown()'s side effect. Takes worker_id because the cancel SQE must
  // be submitted (not just queued) before close() runs, so the fd can't be
  // reused by a new connection out from under a still-pending cancel. Use
  // this only when no response is coming — otherwise use SubmitCancelRecv,
  // which retires the recv without touching the socket itself.
  void CloseConnection(int worker_id, int fd);

  struct io_uring_sqe* GetWorkerIouringSqe(int worker_id, EventDataBase* data);
  struct io_uring_sqe* GetListenIouringSqe(AcceptEvent* data);

  // Buffer Pool
  void InitBufferPool(int worker_id);
  void ProvideBufferToIouring(int worker_id);
  void InitWorkerBuffers(int worker_id);
  uint16_t GetBufferIdFromCqeFlags(uint32_t cqe_flags, RecvEvent* recvEvent);
  // Fire-and-forget: hands buffer_id back to the kernel's pool for worker_id
  // so it can be selected again by a future recv.
  void ReturnBufferToPool(int worker_id, int buffer_id);

};

}  // namespace simple_http_server

#endif  // HTTP_SERVER_H_
