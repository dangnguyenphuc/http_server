#include "http_server.h"

#include <arpa/inet.h>
#include <liburing.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

#include "http_message.h"
#include "uri.h"

#include <iostream>

namespace simple_http_server {

HttpServer::HttpServer(const std::string &host, std::uint16_t port)
    : host_(host),
      port_(port),
      sock_fd_(0),
      running_(false),
      worker_ring_() {
  CreateSocket();
  worker_event_fd_.resize(kThreadPoolSize);
  worker_pending_fds_.resize(kThreadPoolSize);
  std::fill(worker_event_fd_.begin(), worker_event_fd_.end(), -1);
}

void HttpServer::Start() {
  running_ = true;
  SetUpRings();
  listen_thread_ = std::thread(&HttpServer::Listen, this);
}

void HttpServer::CreateSocket() {

  sock_fd_ = socket(
        AF_INET,
        SOCK_STREAM,
        0);

    if (sock_fd_ < 0) {
      throw std::runtime_error("Failed to create a TCP socket");
    }

    int yes = 1;

    setsockopt(
        sock_fd_,
        SOL_SOCKET,
        SO_REUSEADDR,
        &yes,
        sizeof(yes));

    sockaddr_in addr{};

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port_);

    if (bind(
            sock_fd_,
            reinterpret_cast<sockaddr*>(&addr),
            sizeof(addr)) < 0) {
      throw std::runtime_error("Failed to bind socket");
    }

    if (listen(sock_fd_, 1024) < 0) {
      throw std::runtime_error("Failed to listen socket");
    }
}

void HttpServer::SetUpRings() {
  if (io_uring_queue_init(kQueueDepth, &listen_ring_, 0) < 0) {
      throw std::runtime_error("Failed to initialize io_uring for worker");
  }
  for (int i = 0; i < kThreadPoolSize; i++) {
    if (io_uring_queue_init(kQueueDepth, &worker_ring_[i], 0) < 0) {
      throw std::runtime_error("Failed to initialize io_uring for worker");
    }

    worker_event_fd_[i] = eventfd(0, EFD_CLOEXEC);
    if (worker_event_fd_[i] < 0) {
      throw std::runtime_error("Failed to event fd for worker");
    }
    worker_threads_[i] = std::thread(&HttpServer::ProcessEvents, this, i);
  }
}

void HttpServer::Stop() {
  running_ = false;
  // Wake listener.
  shutdown(sock_fd_, SHUT_RDWR);

  // Wake workers.
  for (int i = 0; i < kThreadPoolSize; ++i) {
    uint64_t one = 1;
    write(worker_event_fd_[i], &one, sizeof(one));
  }

  listen_thread_.join();

  for (int i = 0; i < kThreadPoolSize; ++i) {
    worker_threads_[i].join();
  }

  io_uring_queue_exit(&listen_ring_);

  for (int i = 0; i < kThreadPoolSize; ++i) {
    io_uring_queue_exit(&worker_ring_[i]);
  }

  close(sock_fd_);
}

void HttpServer::Listen() {
  int current_worker = 0;
  SubmitAcceptMultishot();
  io_uring_submit(&listen_ring_);
  while (running_) {
    struct io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(
          &listen_ring_,
          &cqe);
    if (ret < 0) {
      fprintf(
          stderr,
          "listen wait_cqe: %s\n",
          strerror(-ret));
      continue;
    }

    // listen_ring_ only ever carries ACCEPT ops.
    auto* data = reinterpret_cast<AcceptEvent*>(cqe->user_data);
    int result = cqe->res;
    if (result >= 0) {
      int client_fd = result;
      DispatchConnection(current_worker, client_fd);
      current_worker = (current_worker + 1) % kThreadPoolSize;
    } else {
      fprintf(
          stderr,
          "accept: %s\n",
          strerror(-result));
    }
    if (!(cqe->flags & IORING_CQE_F_MORE)) {
      delete data;
      if (running_) {
        SubmitAcceptMultishot();
      }
    }
    io_uring_cqe_seen(&listen_ring_, cqe);
    io_uring_submit(&listen_ring_);
  }
}

void HttpServer::SubmitAcceptMultishot() {
  auto* data = new AcceptEvent();
  struct io_uring_sqe* sqe = GetListenIouringSqe(data);
  io_uring_prep_multishot_accept(sqe, sock_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
  io_uring_sqe_set_data(sqe, data);
}

void HttpServer::DispatchConnection(int worker_id, int fd) {
  {
    std::lock_guard<std::mutex> lock(worker_mutex_[worker_id]);
    worker_pending_fds_[worker_id].push_back(fd);
  }
  /*
  * Wake worker.
  */
  uint64_t one = 1;

  ssize_t n = write(worker_event_fd_[worker_id], &one, sizeof(one));
  if (n != sizeof(one)) {
    close(fd);
    throw std::runtime_error("Cannot write to event_fd");
  }
}

void HttpServer::SubmitWorkerNotify(int worker_id) {
  auto* data = new NotifyEvent();
  struct io_uring_sqe* sqe = GetWorkerIouringSqe(worker_id, data);

  io_uring_prep_read(
      sqe,
      worker_event_fd_[worker_id],
      &data->notify_value,
      sizeof(data->notify_value),
      0);

  io_uring_sqe_set_data(sqe, data);
}

void HttpServer::SubmitRecvRequest(int worker_id, IoEvent* data) {
  struct io_uring_sqe* sqe = GetWorkerIouringSqe(worker_id, data);
  // todo: here
  data->type = EventType::READ;
  io_uring_prep_recv(sqe, data->fd, data->buffer, kMaxBufferSize - 1, 0);
  io_uring_sqe_set_data(sqe, data);
}

void HttpServer::SubmitSendRequest(int worker_id, IoEvent* data) {
  struct io_uring_sqe* sqe = GetWorkerIouringSqe(worker_id, data);
  // todo: here
  data->type = EventType::WRITE;
  size_t remaining = data->length - data->cursor;
  io_uring_prep_send(sqe, data->fd, data->buffer + data->cursor, remaining, 0);
  io_uring_sqe_set_data(sqe, data);
}

void HttpServer::HandleNotify(int worker_id) {
  for (;;) {
    int client_fd = -1;
    if (io_uring_sq_space_left(&worker_ring_[worker_id]) <= 0) break;
    {
      std::lock_guard<std::mutex> lock(worker_mutex_[worker_id]);
      std::deque<int>& pending_fds = worker_pending_fds_[worker_id];
      if(pending_fds.empty()) {
        break;
      }
      client_fd = pending_fds.front();
      pending_fds.pop_front();
    }

    auto* data = new IoEvent();
    data->fd = client_fd;
    printf("worker %d: new fd=%d\n", worker_id, client_fd);
    SubmitRecvRequest(worker_id, data);
  }
}

void HttpServer::HandleRecv(int worker_id, int res, IoEvent* data) {
  if (res <= 0) {
    close(data->fd);
    delete data;
    return;
  }

  data->buffer[res] = '\0';
  printf("worker %d fd=%d request:\n%s\n",
      worker_id,
      data->fd,
      data->buffer);

  if (!HandleHttpData(data)) {
    return;
  }
  SubmitSendRequest(worker_id, data);
}

void HttpServer::HandleSend(int worker_id, int res, IoEvent* data) {
  if (res < 0) {
    close(data->fd);
    delete data;
    return;
  }

  data->cursor += static_cast<size_t>(res);

  if (data->cursor < data->length) {
    SubmitSendRequest(worker_id, data);
    return;
  }

  close(data->fd);
  delete data;
}

void HttpServer::ProcessEvents(int worker_id) {
  struct io_uring* ring = &worker_ring_[worker_id];
  SubmitWorkerNotify(worker_id);
  io_uring_submit(ring);
  while (running_) {
    struct io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(ring, &cqe);
    if (ret < 0) {
      fprintf(
          stderr,
          "worker %d wait: %s\n",
          worker_id,
          strerror(-ret)
      );

      continue;
    }

    unsigned head;
    unsigned count = 0;

    io_uring_for_each_cqe(ring, head, cqe) {
      auto* event = reinterpret_cast<EventDataBase*>(io_uring_cqe_get_data(cqe));
      int res = cqe->res;
      HandleURingEvent(worker_id, res, event);
      count += 1;
    }
    io_uring_cq_advance(ring, count);
    HandleNotify(worker_id);
    io_uring_submit(ring);
  }
}

void HttpServer::HandleURingEvent(int worker_id, int res, EventDataBase *event) {
  switch (event->type)
  {
    case EventType::NOTIFY: {
      auto* data = static_cast<NotifyEvent*>(event);
      // Re-arm the eventfd read first so it always gets a slot, even if
      // draining below fills the rest of the submission queue.
      SubmitWorkerNotify(worker_id);
      if (res >= 0) {
        HandleNotify(worker_id);
      }
      delete data;
      break;
    }
    case EventType::READ:
      HandleRecv(worker_id, res, static_cast<IoEvent*>(event));
      break;
    case EventType::WRITE:
      HandleSend(worker_id, res, static_cast<IoEvent*>(event));
      break;
    default:
      break;
  }
}

bool HttpServer::HandleHttpData(IoEvent* raw_request) {
  std::string request_string(raw_request->buffer), response_string;
  HttpRequest http_request;
  HttpResponse http_response;

  try {
    http_request = string_to_request(request_string);
    http_response = HandleHttpRequest(http_request);
  } catch (const std::invalid_argument &e) {
    http_response = HttpResponse(HttpStatusCode::BadRequest);
    http_response.SetContent(e.what());
  } catch (const std::logic_error &e) {
    http_response = HttpResponse(HttpStatusCode::HttpVersionNotSupported);
    http_response.SetContent(e.what());
  } catch (const std::exception &e) {
    http_response = HttpResponse(HttpStatusCode::InternalServerError);
    http_response.SetContent(e.what());
  }

  std::cout << "Received Request:" << to_string(http_request); 

  // Set response to write to client
  response_string =
      to_string(http_response, http_request.method() != HttpMethod::HEAD);
  if (response_string.size() >= kMaxBufferSize) {
    close(raw_request->fd);
    delete raw_request;
    return false;
  }
  std::strncpy(raw_request->buffer, response_string.c_str(), sizeof(char)*kMaxBufferSize - 1);
  raw_request->buffer[kMaxBufferSize - 1] = '\0';
  raw_request->length = response_string.length();
  raw_request->cursor = 0;
  return true;
}

HttpResponse HttpServer::HandleHttpRequest(const HttpRequest &request) {
  auto it = request_handlers_.find(request.uri());
  if (it == request_handlers_.end()) {  // this uri is not registered
    return HttpResponse(HttpStatusCode::NotFound);
  }
  auto callback_it = it->second.find(request.method());
  if (callback_it == it->second.end()) {  // no handler for this method
    return HttpResponse(HttpStatusCode::MethodNotAllowed);
  }
  return callback_it->second(request);  // call handler to process the request
}

struct io_uring_sqe* HttpServer::GetWorkerIouringSqe(int worker_id, NotifyEvent* data) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&worker_ring_[worker_id]);
  if (!sqe) {
    delete data;
    throw std::runtime_error("Failed to get sqe of worker_ring");
  }
  return sqe;
}

struct io_uring_sqe* HttpServer::GetWorkerIouringSqe(int worker_id, IoEvent* data) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&worker_ring_[worker_id]);
  if (!sqe) {
    delete data;
    throw std::runtime_error("Failed to get sqe of worker_ring");
  }
  return sqe;
}

struct io_uring_sqe* HttpServer::GetListenIouringSqe(AcceptEvent* data) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&listen_ring_);
  if (!sqe) {
    delete data;
    throw std::runtime_error("Failed to get sqe of listen_ring");
  }
  return sqe;
}

}  // namespace simple_http_server
