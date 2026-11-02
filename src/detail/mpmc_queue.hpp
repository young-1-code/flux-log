#pragma once
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <utility>
namespace flux::detail {
template <class T>
class mpmc_bounded_queue {
 public:
    mpmc_bounded_queue() = default;
    explicit mpmc_bounded_queue(std::size_t n) { reset(n); }
    void reset(std::size_t n)
    {
        if (n < 2 || (n & (n - 1))) throw std::invalid_argument("queue_items must be a power of two >= 2");
        std::lock_guard l(m_);
        cap_ = n;
        q_.clear();
    }
    bool try_push(T&& v)
    {
        std::lock_guard l(m_);
        if (q_.size() >= cap_) return false;
        q_.push_back(std::move(v));
        cv_.notify_one();
        return true;
    }
    bool try_pop(T& v)
    {
        std::lock_guard l(m_);
        if (q_.empty()) return false;
        v = std::move(q_.front());
        q_.pop_front();
        return true;
    }
    std::size_t size() const noexcept
    {
        std::lock_guard l(m_);
        return q_.size();
    }
    std::size_t capacity() const noexcept { return cap_; }
    void notify() noexcept { cv_.notify_all(); }
    bool wait_pop(T& v, std::chrono::milliseconds d)
    {
        std::unique_lock l(m_);
        if (!cv_.wait_for(l, d, [&] { return !q_.empty(); })) return false;
        v = std::move(q_.front());
        q_.pop_front();
        return true;
    }

 private:
    mutable std::mutex m_;
    std::condition_variable cv_;
    std::deque<T> q_;
    std::size_t cap_{};
};
}  // namespace flux::detail
