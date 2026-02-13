#include "http_server.h"

#include <arpa/inet.h>
#include <liburing.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>

#include "http_message.h"
#include "uri.h"

#include <iostream> // delete this shit

namespace simple_http_server {

HttpServer::HttpServer(const std::string &host, std::uint16_t port)
    : host_(host),
      port_(port),
      sock_fd_(0),
      running_(false),
      worker_ring_(),
      rng_(std::chrono::steady_clock::now().time_since_epoch().count()),
      sleep_times_(10, 100) {
  CreateSocket();
}

void HttpServer::Start() {
  int opt = 1;
  sockaddr_in server_address;

  if (setsockopt(sock_fd_, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt)) < 0) {
    throw std::runtime_error("Failed to set socket options");
  }

  server_address.sin_family = AF_INET;
  server_address.sin_addr.s_addr = INADDR_ANY;
  inet_pton(AF_INET, host_.c_str(), &(server_address.sin_addr.s_addr));
  server_address.sin_port = htons(port_);

  if (bind(sock_fd_, (sockaddr *)&server_address, sizeof(server_address)) < 0) {
    throw std::runtime_error("Failed to bind to socket");
  }

  if (listen(sock_fd_, kBacklogSize) < 0) {
    std::ostringstream msg;
    msg << "Failed to listen on port " << port_;
    throw std::runtime_error(msg.str());
  }

  SetUpRings();
  running_ = true;
  listener_thread_ = std::thread(&HttpServer::Listen, this);
  for (int i = 0; i < kThreadPoolSize; i++) {
    worker_threads_[i] = std::thread(&HttpServer::ProcessEvents, this, i);
  }
}

void HttpServer::CreateSocket() {
  if ((sock_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)) < 0) {
    throw std::runtime_error("Failed to create a TCP socket");
  }
}

void HttpServer::SetUpRings() {
  for (int i = 0; i < kThreadPoolSize; i++) {
    if (io_uring_queue_init(kQueueDepth, &worker_ring_[i], 0) < 0) {
      throw std::runtime_error("Failed to initialize io_uring for worker");
    }
  }
}

void HttpServer::Stop() {
  running_ = false;
  listener_thread_.join();
  for (int i = 0; i < kThreadPoolSize; i++) {
    worker_threads_[i].join();
  }
  for (int i = 0; i < kThreadPoolSize; i++) {
    io_uring_queue_exit(&worker_ring_[i]);
  }
  close(sock_fd_);
}

void HttpServer::Listen() {
  sockaddr_in client_address;
  socklen_t client_len = sizeof(client_address);
  int client_fd;
  int current_worker = 0;
  bool active = true;

  // accept new connections and distribute tasks to worker threads
  while (running_) {
    if (!active) {
      std::this_thread::sleep_for(
          std::chrono::microseconds(sleep_times_(rng_)));
    }
    client_fd = accept4(sock_fd_, (sockaddr *)&client_address, &client_len,
                        SOCK_NONBLOCK);
    if (client_fd < 0) {
      active = false;
      continue;
    }

    active = true;
    SubmitReadRequest(current_worker, client_fd);
    current_worker = (current_worker + 1) % kThreadPoolSize;;
  }
}

void HttpServer::SubmitReadRequest(int worker_id, int client_fd) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&worker_ring_[worker_id]);
  EventData* data = new EventData();
  data->fd = client_fd;
  data->type = EventType::READ;

  io_uring_prep_recv(sqe, client_fd, data->buffer, kMaxBufferSize, 0);
  io_uring_sqe_set_data(sqe, data);
  io_uring_submit(&worker_ring_[worker_id]);
}

void HttpServer::SubmitWriteRequest(int worker_id, EventData* data) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&worker_ring_[worker_id]);
  io_uring_prep_send(sqe, data->fd, data->buffer, data->length, 0);
  io_uring_sqe_set_data(sqe, data);
  io_uring_submit(&worker_ring_[worker_id]);
}

void HttpServer::ProcessEvents(int worker_id) {
  struct io_uring_cqe* cqe;
  struct io_uring* ring = &worker_ring_[worker_id];
  
  bool active = true;

  while (running_) {
    if (!active) {
      std::this_thread::sleep_for(
          std::chrono::microseconds(sleep_times_(rng_)));
    }
    int ret = io_uring_wait_cqe(ring, &cqe);
    if (ret < 0) {
      active = false;
      continue;
    }

    active = true;

    EventData* data = reinterpret_cast<EventData *>(io_uring_cqe_get_data(cqe));
    int res = cqe->res;

    if(res <= 0) {
      close(data->fd);
      delete data;
    } else {
      HandleURingEvent(worker_id, data);
    }

    io_uring_cqe_seen(ring, cqe);
  }
}

void HttpServer::HandleURingEvent(int worker_id, EventData *data) {
  if(data->type == EventType::READ) {
    EventData *response = new EventData();
    response->fd = data->fd;
    response->type = EventType::WRITE;

    HandleHttpData(*data, response);
    SubmitWriteRequest(worker_id, response);
    delete data;
  } else if (data->type == EventType::WRITE) {
    close(data->fd);
    delete data;
  }
}

void HttpServer::HandleHttpData(const EventData &raw_request,
                                EventData *raw_response) {
  std::string request_string(raw_request.buffer), response_string;
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
  memcpy(raw_response->buffer, response_string.c_str(), kMaxBufferSize);
  raw_response->length = response_string.length();
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

}  // namespace simple_http_server
