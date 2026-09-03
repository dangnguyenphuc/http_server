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
  worker_buffer_pool_.resize(kThreadPoolSize);
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

void HttpServer::InitBufferPool(int worker_id) {
  // resize() value-initializes new elements, which zeros ProvidedBuffer::data.
  worker_buffer_pool_[worker_id].resize(kMaxBufferPoolSize);
}

void HttpServer::ProvideBufferToIouring(int worker_id) {
  auto& buffers = worker_buffer_pool_[worker_id];
  struct io_uring_sqe* sqe = GetWorkerIouringSqe(worker_id, nullptr);
  io_uring_prep_provide_buffers(
    sqe,
    buffers[0].data,
    kMaxBufferSize,
    static_cast<int>(buffers.size()),
    static_cast<uint16_t>(worker_id),
    0
  );
  io_uring_sqe_set_data(sqe, nullptr);
}

void HttpServer::InitWorkerBuffers(int worker_id) {
  ProvideBufferToIouring(worker_id);
  io_uring_submit(&worker_ring_[worker_id]);
  struct io_uring_cqe* cqe = nullptr;
  int ret = io_uring_wait_cqe(&worker_ring_[worker_id], &cqe);

  if(ret < 0) {
    throw std::runtime_error("Failed waiting for provide buffers");
  }

  if (cqe->res < 0) {
    int err = -cqe->res;
    io_uring_cqe_seen(&worker_ring_[worker_id], cqe);
    throw std::runtime_error(std::string("provide buffers failed: ") + strerror(err));
  }

  io_uring_cqe_seen(&worker_ring_[worker_id], cqe);
}

void HttpServer::SetUpRings() {
  if (io_uring_queue_init(kQueueDepth, &listen_ring_, 0) < 0) {
      throw std::runtime_error("Failed to initialize io_uring for worker");
  }
  for (int i = 0; i < kThreadPoolSize; i++) {
    if (io_uring_queue_init(kQueueDepth, &worker_ring_[i], 0) < 0) {
      throw std::runtime_error("Failed to initialize io_uring for worker");
    }

    InitBufferPool(i);
    InitWorkerBuffers(i);

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
  struct io_uring_sqe* sqe = GetWorkerIouringSqe(worker_id, static_cast<EventDataBase*>(data));

  io_uring_prep_read(
      sqe,
      worker_event_fd_[worker_id],
      &data->notify_value,
      sizeof(data->notify_value),
      0);

  io_uring_sqe_set_data(sqe, data);
}

void HttpServer::SubmitRecvRequestMultishot(int worker_id, int client_fd) {
  RecvEvent* data = new RecvEvent();
  data->fd = client_fd;
  struct io_uring_sqe* sqe = GetWorkerIouringSqe(worker_id, static_cast<EventDataBase*>(data));
  // todo: here
  data->type = EventType::RECV;
  io_uring_prep_recv_multishot(sqe, data->fd, nullptr, 0, 0);
  io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT);
  io_uring_sqe_set_buf_group(sqe, static_cast<uint16_t>(worker_id));
  io_uring_sqe_set_data(sqe, data);
}

void HttpServer::SubmitSendRequest(int worker_id, uint16_t buffer_id, int len, int client_fd) {
  SendEvent* data = new SendEvent();
  data->buffer_id = buffer_id;
  data->fd = client_fd;
  data->length = len;
  data->type = EventType::SEND;
  auto& buffer = worker_buffer_pool_[worker_id][buffer_id].data;
  struct io_uring_sqe* sqe = GetWorkerIouringSqe(worker_id, static_cast<EventDataBase*>(data));
  size_t remaining = data->length - data->cursor;
  io_uring_prep_send(sqe, data->fd, buffer + data->cursor, remaining, 0);
  io_uring_sqe_set_data(sqe, data);
}

void HttpServer::SubmitRemainSendRequest(int worker_id, SendEvent* data) {
  struct io_uring_sqe* sqe = GetWorkerIouringSqe(worker_id, static_cast<EventDataBase*>(data));
  size_t remaining = data->length - data->cursor;
  auto& buffer = worker_buffer_pool_[worker_id][data->buffer_id].data;
  io_uring_prep_send(sqe, data->fd, buffer + data->cursor, remaining, 0);
  io_uring_sqe_set_data(sqe, data);
}

void HttpServer::SubmitCancelRecv(int worker_id, RecvEvent* data) {
  struct io_uring_sqe* sqe = GetWorkerIouringSqe(worker_id, nullptr);
  io_uring_prep_cancel_fd(sqe, data->fd, 0);
  io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS);
  io_uring_sqe_set_data(sqe, nullptr);
  io_uring_submit(&worker_ring_[worker_id]);
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
    SubmitRecvRequestMultishot(worker_id, client_fd);
  }
}

void HttpServer::HandleRecv(int worker_id, int res, RecvEvent* data, uint32_t cqe_flags) {
  bool more = cqe_flags & IORING_CQE_F_MORE;

  if (!data->recv_active) {
    if (cqe_flags & IORING_CQE_F_BUFFER) {
      ReturnBufferToPool(worker_id, cqe_flags >> IORING_CQE_BUFFER_SHIFT);
    }
    if (!more) {
      if (data->pending_len > 0) {
        SubmitSendRequest(worker_id, data->pending_buffer_id, data->pending_len, data->fd);
      }
      delete data;
    }
    return;
  }

  if (res <= 0 || res >= static_cast<int>(kMaxBufferSize)) {
    data->recv_active = false;
    if (!data->cancelled) {
      CloseConnection(worker_id, data->fd);
      data->cancelled = true;
    }
    if (!more) {
      delete data;
    }
    return;
  }

  uint16_t buffer_id = GetBufferIdFromCqeFlags(cqe_flags, data);
  worker_buffer_pool_[worker_id][buffer_id].data[res] = '\0';
  int len = HandleHttpData(worker_buffer_pool_[worker_id][buffer_id].data);

  data->recv_active = false;
  if (!len) {
    ReturnBufferToPool(worker_id, buffer_id);
    CloseConnection(worker_id, data->fd);
    data->cancelled = true;
  } else {
    data->pending_buffer_id = buffer_id;
    data->pending_len = len;
    SubmitCancelRecv(worker_id, data);
    data->cancelled = true;
  }

  if (!more) {
    if (data->pending_len > 0) {
      SubmitSendRequest(worker_id, data->pending_buffer_id, data->pending_len, data->fd);
    }
    delete data;
  }
}

void HttpServer::HandleSend(int worker_id, int res, SendEvent* data) {
  if (res < 0) {
    CloseConnection(worker_id, data->fd);
    ReturnBufferToPool(worker_id, data->buffer_id);
    delete data;
    return;
  }

  data->cursor += static_cast<size_t>(res);

  if (data->cursor < data->length) {
    SubmitRemainSendRequest(worker_id, data);
    return;
  }
  ReturnBufferToPool(worker_id, data->buffer_id);
  CloseConnection(worker_id, data->fd);
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
      if (event) {
        HandleURingEvent(worker_id, event, res, cqe->flags);
      } else if (res < 0) {
        // A fire-and-forget op (e.g. ReturnBufferToPool) failed.
        fprintf(stderr, "worker %d: untracked op failed: %s\n", worker_id, strerror(-res));
      }
      count += 1;
    }
    io_uring_cq_advance(ring, count);
    HandleNotify(worker_id);
    io_uring_submit(ring);
  }
}

void HttpServer::HandleURingEvent(int worker_id, EventDataBase *event, int res, uint32_t cqe_flags) {
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
    case EventType::RECV:
      HandleRecv(worker_id, res, static_cast<RecvEvent*>(event), cqe_flags);
      break;
    case EventType::SEND:
      HandleSend(worker_id, res, static_cast<SendEvent*>(event));
      break;
    default:
      break;
  }
}

int HttpServer::HandleHttpData(char* buffer) {
  std::string request_string(buffer), response_string;
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
    fprintf(stderr, "Invalid response string: %s", response_string.c_str());
    return 0;
  }
  std::strncpy(buffer, response_string.c_str(), sizeof(char)*kMaxBufferSize - 1);
  buffer[kMaxBufferSize - 1] = '\0';
  return response_string.length();
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

void HttpServer::CloseConnection(int worker_id, int fd) {
  shutdown(fd, SHUT_RDWR);
  struct io_uring_sqe* sqe = GetWorkerIouringSqe(worker_id, nullptr);
  io_uring_prep_cancel_fd(sqe, fd, 0);
  io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS);
  io_uring_sqe_set_data(sqe, nullptr);
  io_uring_submit(&worker_ring_[worker_id]);

  close(fd);
}

struct io_uring_sqe* HttpServer::GetWorkerIouringSqe(int worker_id, EventDataBase* data) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&worker_ring_[worker_id]);
  if (!sqe) {
    if (data) delete data;
    throw std::runtime_error("Failed to get sqe of worker_ring");
  }
  return sqe;
}

struct io_uring_sqe* HttpServer::GetListenIouringSqe(AcceptEvent* data) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&listen_ring_);
  if (!sqe) {
    if (data) delete data;
    throw std::runtime_error("Failed to get sqe of listen_ring");
  }
  return sqe;
}

uint16_t HttpServer::GetBufferIdFromCqeFlags(uint32_t cqe_flags, RecvEvent* recvEvent) {
  if (!(cqe_flags & IORING_CQE_F_BUFFER)) {
    close(recvEvent->fd);
    delete recvEvent;
    throw std::runtime_error("Worker CQE did not select a provided buffer");
  }
  return cqe_flags >> IORING_CQE_BUFFER_SHIFT;
}

void HttpServer::ReturnBufferToPool(int worker_id, int buffer_id) {
  struct io_uring_sqe* sqe = GetWorkerIouringSqe(worker_id, nullptr);
  io_uring_prep_provide_buffers(
      sqe,
      worker_buffer_pool_[worker_id][buffer_id].data,
      kMaxBufferSize,
      1,
      static_cast<uint16_t>(worker_id),
      buffer_id);
  io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS);
  io_uring_sqe_set_data(sqe, nullptr);
}

}  // namespace simple_http_server
