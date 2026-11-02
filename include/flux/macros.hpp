#pragma once
#include "registry.hpp"
#define FLUX_LOG(l, lvl, fmt, ...)                                                                              \
    do {                                                                                                        \
        if ((l).should_log(lvl)) (l).log(lvl, std::source_location::current(), fmt __VA_OPT__(, ) __VA_ARGS__); \
    } while (0)
#define FLUX_TRACE(l, f, ...) FLUX_LOG(l, flux::Level::Trace, f __VA_OPT__(, ) __VA_ARGS__)
#define FLUX_DEBUG(l, f, ...) FLUX_LOG(l, flux::Level::Debug, f __VA_OPT__(, ) __VA_ARGS__)
#define FLUX_INFO(l, f, ...) FLUX_LOG(l, flux::Level::Info, f __VA_OPT__(, ) __VA_ARGS__)
#define FLUX_WARN(l, f, ...) FLUX_LOG(l, flux::Level::Warn, f __VA_OPT__(, ) __VA_ARGS__)
#define FLUX_ERROR(l, f, ...) FLUX_LOG(l, flux::Level::Error, f __VA_OPT__(, ) __VA_ARGS__)
#define FLUX_CRITICAL(l, f, ...) FLUX_LOG(l, flux::Level::Critical, f __VA_OPT__(, ) __VA_ARGS__)
