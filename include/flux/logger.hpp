#pragma once
#include <format>
#include <iterator>
#include <source_location>
#include <string_view>
#include <utility>
#include <vector>
#include "config.hpp"
#include "level.hpp"
namespace flux {
class Logger;
namespace detail {
void emit(Logger&, Level, std::source_location, std::string) noexcept;
void record_format_failed() noexcept;
}  // namespace detail
class Logger {
 public:
    // 热路径只做等级判断；默认阈值为 Info。
    bool should_log(Level l) const noexcept { return static_cast<unsigned>(l) >= static_cast<unsigned>(level_); }
    void set_level(Level l) noexcept { level_ = l; }
    void set_overflow(OverflowPolicy p) noexcept { policy_ = p; }
    // 参数在生产线程展开，后端只负责行组装和 I/O。
    template <class... A>
    void log(Level l, std::source_location loc, std::format_string<A...> fmt, A&&... a) noexcept
    {
        try {
            std::string result;
            result.reserve(256);
            std::format_to(std::back_inserter(result), fmt, std::forward<A>(a)...);
            detail::emit(*this, l, loc, std::move(result));
        } catch (...) {
            detail::record_format_failed();
        }
    }

 private:
    friend Logger& get(std::string_view);
    friend void init(Config);
    friend void shutdown(std::chrono::milliseconds);
    std::string name_;
    std::uint16_t name_id_{0};
    Level level_{Level::Info};
    OverflowPolicy policy_{OverflowPolicy::DropNewest};
    friend void detail::emit(Logger&, Level, std::source_location, std::string) noexcept;
};
}  // namespace flux
