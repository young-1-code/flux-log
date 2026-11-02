#pragma once
#include "logger.hpp"
#include "sink.hpp"
namespace flux {
// 唯一后端的生命周期入口；Logger 和符号表跨多轮 init 复用。
void init(Config);
Logger& get(std::string_view);
void flush(std::chrono::milliseconds timeout = std::chrono::seconds(1));
void shutdown(std::chrono::milliseconds timeout = std::chrono::seconds(5));
Stats stats();
}  // namespace flux
