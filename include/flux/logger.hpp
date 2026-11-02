#pragma once
#include <source_location>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>
#include "config.hpp"
#include "level.hpp"
namespace flux {
class Logger;
namespace detail {
void emit(Logger&, Level, std::source_location, std::string) noexcept;
}
class Logger {
 public:
    bool should_log(Level l) const noexcept { return static_cast<unsigned>(l) >= static_cast<unsigned>(level_); }
    void set_level(Level l) noexcept { level_ = l; }
    void set_overflow(OverflowPolicy p) noexcept { policy_ = p; }
    template <class... A>
    void log(Level l, std::source_location loc, std::string_view fmt, A&&... a) noexcept
    {
        try {
            std::vector<std::string> vals;
            vals.reserve(sizeof...(A));
            (vals.emplace_back(([&] {
                std::ostringstream x;
                x << a;
                return x.str();
            })()),
             ...);
            std::string result;
            std::size_t p = 0, n = 0;
            while (p < fmt.size()) {
                auto q = fmt.find("{}", p);
                if (q == std::string_view::npos) {
                    result.append(fmt.substr(p));
                    break;
                }
                result.append(fmt.substr(p, q - p));
                result += n < vals.size() ? vals[n++] : "{}";
                p = q + 2;
            }
            detail::emit(*this, l, loc, std::move(result));
        } catch (...) {
        }
    }

 private:
    friend Logger& get(std::string_view);
    friend void init(Config);
    friend void shutdown(std::chrono::milliseconds);
    std::string name_;
    Level level_{Level::Info};
    OverflowPolicy policy_{OverflowPolicy::DropNewest};
    friend void detail::emit(Logger&, Level, std::source_location, std::string) noexcept;
};
}  // namespace flux
