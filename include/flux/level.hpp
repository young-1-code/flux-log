#pragma once
#include <cstdint>
#include <string_view>
namespace flux {
enum class Level : std::uint8_t { Trace, Debug, Info, Warn, Error, Panic, Off };
constexpr std::string_view level_str(Level v) noexcept
{
    switch (v) {
        case Level::Trace:
            return "trace";
        case Level::Debug:
            return "debug";
        case Level::Info:
            return "info";
        case Level::Warn:
            return "warn";
        case Level::Error:
            return "error";
        case Level::Panic:
            return "panic";
        default:
            return "off";
    }
}
}  // namespace flux
