#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace flux::detail {
// Vyukov bounded MPMC queue adapted from the repository's original queue.
// Atomics protect the data path; the condition variable only parks idle waiters.
template <class T>
class mpmc_bounded_queue {
 public:
    mpmc_bounded_queue() noexcept = default;
    explicit mpmc_bounded_queue(std::size_t n) { reset(n); }
    mpmc_bounded_queue(const mpmc_bounded_queue&) = delete;
    mpmc_bounded_queue& operator=(const mpmc_bounded_queue&) = delete;

    void reset(std::size_t n)
    {
        if (n < 2 || (n & (n - 1)) != 0) throw std::invalid_argument("queue_items must be a power of two >= 2");
        auto next = std::make_unique<cell[]>(n);
        for (std::size_t i = 0; i < n; ++i)
            next[i].sequence.store(i, std::memory_order_relaxed);
        enqueue_pos_.store(0, std::memory_order_relaxed);
        dequeue_pos_.store(0, std::memory_order_relaxed);
        buffer_ = std::move(next);
        mask_ = n - 1;
        capacity_ = n;
        notify();
    }

    bool try_push(T&& value) noexcept
    {
        if (!buffer_) return false;
        cell* c;
        std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            c = &buffer_[pos & mask_];
            const auto seq = c->sequence.load(std::memory_order_acquire);
            const auto dif = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
            if (dif == 0) {
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) break;
            } else if (dif < 0) {
                return false;
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }
        c->data = std::move(value);
        c->sequence.store(pos + 1, std::memory_order_release);
        notify();
        return true;
    }

    bool try_pop(T& value) noexcept
    {
        if (!buffer_) return false;
        cell* c;
        std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            c = &buffer_[pos & mask_];
            const auto seq = c->sequence.load(std::memory_order_acquire);
            const auto dif = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
            if (dif == 0) {
                if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) break;
            } else if (dif < 0) {
                return false;
            } else {
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }
        value = std::move(c->data);
        c->sequence.store(pos + mask_ + 1, std::memory_order_release);
        return true;
    }

    std::size_t size() const noexcept
    {
        if (!buffer_) return 0;
        const auto result = enqueue_pos_.load(std::memory_order_acquire) - dequeue_pos_.load(std::memory_order_acquire);
        return result < capacity_ ? result : capacity_;
    }
    std::size_t capacity() const noexcept { return capacity_; }

    bool wait_pop(T& value, std::chrono::milliseconds timeout)
    {
        if (try_pop(value)) return true;
        std::unique_lock lock(wait_mutex_);
        wait_cv_.wait_for(lock, timeout, [&] { return size() != 0; });
        return try_pop(value);
    }
    void notify() noexcept { wait_cv_.notify_one(); }

 private:
    struct cell {
        std::atomic<std::size_t> sequence{0};
        T data{};
    };
    std::unique_ptr<cell[]> buffer_;
    std::size_t mask_{0};
    std::size_t capacity_{0};
    alignas(64) std::atomic<std::size_t> enqueue_pos_{0};
    alignas(64) std::atomic<std::size_t> dequeue_pos_{0};
    mutable std::mutex wait_mutex_;
    std::condition_variable wait_cv_;
};
}  // namespace flux::detail
