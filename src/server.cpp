#include <iostream>
#include <vector>
#include <atomic>
#include <thread>
#include <chrono>
#include <memory>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in&gt;
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <immintrin.h>

#define CACHE_LINE_SIZE 64

struct alignas(CACHE_LINE_SIZE) TokenBucketState {
    std::atomic<uint64_t> tokens;
    std::atomic<uint64_t> last_update_ns;
    uint32_t capacity_scaled;
    uint32_t refill_rate_scaled;
};

class SlabAllocator {
public:
    SlabAllocator(size_t size) : pool_size(size), offset(0) {
        buffer = malloc(pool_size);
        if (!buffer) {
            throw std::runtime_error("Failed to allocate slab memory");
        }
        std::memset(buffer, 0, pool_size);
    }

    ~SlabAllocator() {
        if (buffer) {
            free(buffer);
        }
    }

    void* allocate(size_t size) {
        size_t current_offset = offset.load(std::memory_order_relaxed);
        while (true) {
            size_t new_offset = current_offset + size;
            if (new_offset > pool_size) {
                return nullptr;
            }
            if (offset.compare_exchange_weak(current_offset, new_offset, std::memory_order_release, std::memory_order_relaxed)) {
                return static_cast<char*>(buffer) + current_offset;
            }
        }
    }

    void reset() {
        offset.store(0, std::memory_order_release);
    }

private:
    void* buffer;
    size_t pool_size;
    std::atomic<size_t> offset;
};

class DistributedTokenServer {
public:
    DistributedTokenServer(int port, uint64_t capacity, uint64_t refill_rate)
        : port(port), running(true), allocator(1024 * 1024 * 64) {
        bucket.tokens.store(capacity, std::memory_order_relaxed);
        bucket.last_update_ns.store(get_monotonic_time_ns(), std::memory_order_relaxed);
        bucket.capacity_scaled = static_cast<uint32_t>(capacity);
        bucket.refill_rate_scaled = static_cast<uint32_t>(refill_rate);
    }

    ~DistributedTokenServer() {
        stop();
    }

    void start() {
        server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0) {
            throw std::runtime_error("Failed to create socket");
        }

        int opt = 1;
        setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port);

        if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
            close(server_fd);
            throw std::runtime_error("Failed to bind socket");
        }

        if (listen(server_fd, SOMAXCONN) < 0) {
            close(server_fd);
            throw std::runtime_error("Failed to listen on socket");
        }

        set_nonblocking(server_fd);

        epoll_fd = epoll_create1(0);
        if (epoll_fd < 0) {
            close(server_fd);
            throw std::runtime_error("Failed to create epoll descriptor");
        }

        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = server_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) < 0) {
            close(server_fd);
            close(epoll_fd);
            throw std::runtime_error("Failed to add server fd to epoll");
        }

        worker_thread = std::thread(&DistributedTokenServer::event_loop, this);
    }

    void stop() {
        if (!running.exchange(false)) {
            return;
        }
        if (server_fd >= 0) {
            close(server_fd);
        }
        if (epoll_fd >= 0) {
            close(epoll_fd);
        }
        if (worker_thread.joinable()) {
            worker_thread.join();
        }
    }

private:
    int port;
    int server_fd = -1;
    int epoll_fd = -1;
    std::atomic<bool> running;
    std::thread worker_thread;
    TokenBucketState bucket;
    SlabAllocator allocator;

    static uint64_t get_monotonic_time_ns() {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + ts.tv_nsec;
    }

    void set_nonblocking(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    bool consume_token() {
        uint32_t backoff_count = 0;
        while (true) {
            uint64_t current_time = get_monotonic_time_ns();
            uint64_t last_time = bucket.last_update_ns.load(std::memory_order_acquire);
            
            if (current_time > last_time) {
                uint64_t elapsed_ns = current_time - last_time;
                uint64_t generated_tokens = (elapsed_ns * bucket.refill_rate_scaled) / 1000000000ULL;
                
                if (generated_tokens > 0) {
                    uint64_t current_tokens = bucket.tokens.load(std::memory_order_relaxed);
                    uint64_t new_tokens = std::min(static_cast<uint64_t>(bucket.capacity_scaled), current_tokens + generated_tokens);
                    
                    if (bucket.tokens.compare_exchange_weak(current_tokens, new_tokens, std::memory_order_release, std::memory_order_relaxed)) {
                        bucket.last_update_ns.compare_exchange_strong(last_time, current_time, std::memory_order_release, std::memory_order_relaxed);
                    }
                }
            }

            uint64_t tokens = bucket.tokens.load(std::memory_order_acquire);
            if (tokens == 0) {
                return false;
            }

            if (bucket.tokens.compare_exchange_weak(tokens, tokens - 1, std::memory_order_release, std::memory_order_relaxed)) {
                return true;
            }

            for (uint32_t i = 0; i < (1U << backoff_count); ++i) {
                _mm_pause();
            }
            if (backoff_count < 6) {
                backoff_count++;
            }
        }
    }

    void event_loop() {
        const int MAX_EVENTS = 64;
        epoll_event events[MAX_EVENTS];

        while (running.load(std::memory_order_relaxed)) {
            int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 100);
            for (int i = 0; i < nfds; ++i) {
                if (events[i].data.fd == server_fd) {
                    while (true) {
                        sockaddr_in client_addr{};
                        socklen_t client_len = sizeof(client_addr);
                        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
                        if (client_fd < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            break;
                        }
                        set_nonblocking(client_fd);
                        epoll_event ev{};
                        ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
                        ev.data.fd = client_fd;
                        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
                    }
                } else {
                    int client_fd = events[i].data.fd;
                    char buf[256];
                    ssize_t bytes_read = read(client_fd, buf, sizeof(buf) - 1);
                    if (bytes_read > 0) {
                        bool allowed = consume_token();
                        const char* response = allowed ? "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK" : "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 9\r\n\r\nTHROTTLED";
                        write(client_fd, response, std::strlen(response));
                    }
                    close(client_fd);
                }
            }
        }
    }
};

int main() {
    try {
        DistributedTokenServer server(8080, 1000, 100);
        server.start();
        std::this_thread::sleep_for(std::chrono::seconds(10));
        server.stop();
    } catch (const std::exception& e) {
        std::cerr << "Server error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}