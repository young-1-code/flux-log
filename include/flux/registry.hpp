#pragma once
#include "logger.hpp"
#include "sink.hpp"
namespace flux {
void init(Config);
Logger& get(std::string_view);
void flush(std::chrono::milliseconds timeout = std::chrono::seconds(1));
void shutdown(std::chrono::milliseconds timeout = std::chrono::seconds(5));
Stats stats();
}  // namespace flux
