#pragma once

#include <atomic>
#include <cassert>
#include <limits>
#include <stdexcept>
#include <memory>
#include <cstdio>
#include <cstring>
#include <utility>


namespace tiny
{
    inline std::string errno_str(int err_num)
    {
        char buf[256] = {0};
        constexpr auto buf_size = sizeof(buf);

        return std::string(strerror_r(err_num, buf, buf_size));
    }

    class mpmc_exception: public std::exception
    {
    public:
        mpmc_exception(const std::string& msg) :
            _msg(msg)
        {}
        mpmc_exception(const std::string& msg, int last_errno)
        {
            _msg = msg + ": " + errno_str(last_errno);
        }
        const char* what() const noexcept override
        {
            return _msg.c_str();
        }
    private:
        std::string _msg;

    };

    /*
     * A modified version of Bounded MPMC queue by Dmitry Vyukov.
     * Original code from:
     * http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
     *
     * Vyukov MPMC Cycle Queue
     */
    template<typename T>
    class mpmc_bounded_queue
    {
    public:

        using item_type = T;
        mpmc_bounded_queue() :
            _buffer(nullptr)
        {
        }

        mpmc_bounded_queue(size_t buffer_size) :
            _max_size(buffer_size),
            _buffer(new cell_t [buffer_size]),
            _buffer_mask(buffer_size - 1)
        {
            //queue size must be power of two
            if (!((buffer_size >= 2) && ((buffer_size & (buffer_size - 1)) == 0)))
            {
                throw mpmc_exception("async queue size must be power of two");
            }

            for (size_t i = 0; i != buffer_size; i += 1)
            {
                _buffer[i]._sequence.store(i, std::memory_order_relaxed);
            }
            _enqueue_pos.store(0, std::memory_order_relaxed);
            _dequeue_pos.store(0, std::memory_order_relaxed);
        }


        void init(size_t buffer_size = 8192)
        {
            _max_size = buffer_size;
            _buffer = new cell_t [buffer_size];
            _buffer_mask = (buffer_size - 1);

            //queue size must be power of two
            if (!((buffer_size >= 2) && ((buffer_size & (buffer_size - 1)) == 0)))
            {
                throw mpmc_exception("async queue size must be power of two");
            }

            for (size_t i = 0; i != buffer_size; i += 1)
            {
                _buffer[i]._sequence.store(i, std::memory_order_relaxed);
            }
            _enqueue_pos.store(0, std::memory_order_relaxed);
            _dequeue_pos.store(0, std::memory_order_relaxed);
        }



        ~mpmc_bounded_queue()
        {
            if(_buffer)
            {
                delete [] _buffer;
                _buffer = nullptr;
            }
        }


        bool try_push(T&& data)
        {
            cell_t* cell;
            size_t pos = _enqueue_pos.load(std::memory_order_relaxed);
            for (;;)
            {
                cell = &_buffer[pos & _buffer_mask];
                size_t seq = cell->_sequence.load(std::memory_order_acquire);
                intptr_t dif = (intptr_t)seq - (intptr_t)pos;
                if (dif == 0)
                {
                    if (_enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                        break;
                }
                else if (dif < 0)
                {
                    return false;
                }
                else
                {
                    pos = _enqueue_pos.load(std::memory_order_relaxed);
                }
            }
            cell->_data = std::move(data);
            cell->_sequence.store(pos + 1, std::memory_order_release);
            return true;
        }

        bool try_pop(T& data)
        {
            cell_t* cell;
            size_t pos = _dequeue_pos.load(std::memory_order_relaxed);
            for (;;)
            {
                cell = &_buffer[pos & _buffer_mask];
                size_t seq = cell->_sequence.load(std::memory_order_acquire);
                intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
                if (dif == 0)
                {
                    if (_dequeue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                        break;
                }
                else if (dif < 0)
                    return false;
                else
                    pos = _dequeue_pos.load(std::memory_order_relaxed);
            }
            data = std::move(cell->_data);
            cell->_sequence.store(pos + _buffer_mask + 1, std::memory_order_release);
            return true;
        }

        size_t size()
        {
            size_t first_pos = _dequeue_pos.load(std::memory_order_relaxed);
            size_t last_pos = _enqueue_pos.load(std::memory_order_relaxed);
            if (last_pos <= first_pos)
                return 0;
            auto size = last_pos - first_pos;
            return size < _max_size ? size : _max_size;
        }

        size_t capacity()
        {
            return _max_size;
        }
    private:
        struct cell_t
        {
            std::atomic<size_t>   _sequence;
            T                     _data;
        };

        size_t                  _max_size;

        static size_t const     cacheline_size = 64;
        typedef char            cacheline_pad_t [cacheline_size];

        cacheline_pad_t         _pad0;
        cell_t*                 _buffer;
        size_t                  _buffer_mask;
        cacheline_pad_t         _pad1;
        std::atomic<size_t>     _enqueue_pos;
        cacheline_pad_t         _pad2;
        std::atomic<size_t>     _dequeue_pos;
        cacheline_pad_t         _pad3;

        mpmc_bounded_queue(mpmc_bounded_queue const&) = delete;
        void operator= (mpmc_bounded_queue const&) = delete;
    };

}
