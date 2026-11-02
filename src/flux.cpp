#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <thread>
#include <unordered_map>
#include "detail/mpmc_queue.hpp"
#include "flux/formatter.hpp"
#include "flux/log.hpp"

namespace flux::detail {
class SymbolTable {
    mutable std::mutex m_;
    std::vector<std::string> v_{""};
    std::unordered_map<std::string, uint16_t> ids_;

 public:
    uint16_t intern(std::string_view s) noexcept
    {
        std::lock_guard l(m_);
        auto it = ids_.find(std::string(s));
        if (it != ids_.end()) return it->second;
        if (v_.size() >= 65536) return 0;
        uint16_t id = (uint16_t)v_.size();
        v_.emplace_back(s);
        ids_[v_.back()] = id;
        return id;
    }
    std::string_view resolve(uint16_t id) const noexcept
    {
        std::lock_guard l(m_);
        return id < v_.size() ? v_[id] : std::string_view{};
    }
};
struct LogItem {
    uint64_t ns{};
    uint32_t tid{};
    uint16_t text_len{};
    Level level{Level::Info};
    uint16_t name_id{};
    uint16_t file_id{};
    uint32_t line{};
    char text[256]{};
    std::unique_ptr<char[]> spill;
    std::string_view view() const noexcept
    {
        return spill ? std::string_view(spill.get(), text_len) : std::string_view(text, text_len);
    }
};
static_assert(sizeof(LogItem) == 288, "LogItem ABI must remain 288 bytes");
}  // namespace flux::detail

namespace {
using flux::detail::LogItem;
using flux::detail::SymbolTable;
std::string basename(std::string_view s)
{
    auto p = s.find_last_of("/\\");
    return std::string(s.substr(p == std::string_view::npos ? 0 : p + 1));
}
std::string replace_args(std::string_view f, const std::vector<std::string>& a)
{
    std::string o;
    size_t pos = 0, ai = 0;
    while (pos < f.size()) {
        auto p = f.find("{}", pos);
        if (p == std::string_view::npos) {
            o.append(f.substr(pos));
            break;
        }
        o.append(f.substr(pos, p - pos));
        if (ai < a.size())
            o += a[ai++];
        else
            o += "{}";
        pos = p + 2;
    }
    return o;
}
class FileSink final : public flux::Sink {
 protected:
    std::string path_;
    std::ofstream out_;
    std::string buf_;
    size_t limit_;
    std::optional<flux::Rotate> rot_;

 public:
    FileSink(std::string p, size_t b, std::optional<flux::Rotate> r) : path_(std::move(p)), limit_(b), rot_(r)
    {
        out_.open(path_, std::ios::app | std::ios::binary);
        if (!out_) throw std::system_error(errno, std::generic_category(), "open log");
    }
    bool write(std::span<const std::string_view> ls) noexcept override
    {
        try {
            size_t n = 0;
            for (auto s : ls)
                n += s.size();
            if (rot_ && rot_->max_size && std::filesystem::file_size(path_) + buf_.size() + n > rot_->max_size) {
                flush();
                out_.close();
                for (unsigned i = rot_->keep; i > 0; --i) {
                    auto a = path_ + "." + std::to_string(i), b = path_ + "." + std::to_string(i + 1);
                    if (i == rot_->keep)
                        std::filesystem::remove(a);
                    else if (std::filesystem::exists(a))
                        std::filesystem::rename(a, b);
                }
                if (std::filesystem::exists(path_)) std::filesystem::rename(path_, path_ + ".1");
                out_.open(path_, std::ios::trunc | std::ios::binary);
            }
            for (auto s : ls)
                buf_.append(s);
            if (buf_.size() >= limit_) return flush();
            return true;
        } catch (...) {
            return false;
        }
    }
    bool flush() noexcept override
    {
        try {
            out_ << buf_;
            out_.flush();
            bool ok = !!out_;
            buf_.clear();
            return ok;
        } catch (...) {
            return false;
        }
    }
    std::string name() const override { return path_; }
};
class StdoutSink final : public flux::Sink {
    FILE* f_;

 public:
    explicit StdoutSink(FILE* f) : f_(f) {}
    bool write(std::span<const std::string_view> ls) noexcept override
    {
        for (auto s : ls)
            if (std::fwrite(s.data(), 1, s.size(), f_) != s.size()) return false;
        return true;
    }
    bool flush() noexcept override { return std::fflush(f_) == 0; }
    std::string name() const override { return "stdout"; }
};
class DailySink final : public flux::Sink {
    std::string base_;
    unsigned keep_;
    flux::DailyClock clock_;
    std::string day_;
    std::shared_ptr<FileSink> current_;
    std::string path_for(const std::string& d) const
    {
        auto p = base_.find("{d}");
        if (p != std::string::npos) {
            auto x = base_;
            x.replace(p, 3, d);
            return x;
        }
        auto dot = base_.find_last_of('.');
        return dot == std::string::npos ? base_ + "." + d : base_.substr(0, dot) + "." + d + base_.substr(dot);
    }
    std::string today() const
    {
        auto t = std::chrono::system_clock::to_time_t(clock_());
        std::tm tm{};
        localtime_r(&t, &tm);
        char b[16];
        std::strftime(b, sizeof b, "%Y-%m-%d", &tm);
        return b;
    }

 public:
    DailySink(std::string p, unsigned k, flux::DailyClock c)
        : base_(std::move(p)),
          keep_(k),
          clock_(c ? std::move(c) : flux::DailyClock([] { return std::chrono::system_clock::now(); }))
    {
        day_ = today();
        current_ = std::make_shared<FileSink>(path_for(day_), 1 << 20, std::nullopt);
    }
    bool write(std::span<const std::string_view> ls) noexcept override
    {
        try {
            auto d = today();
            if (d != day_) {
                current_->flush();
                day_ = d;
                current_ = std::make_shared<FileSink>(path_for(day_), 1 << 20, std::nullopt);
            }
            return current_->write(ls);
        } catch (...) {
            return false;
        }
    }
    bool flush() noexcept override { return current_->flush(); }
    std::string name() const override { return path_for(day_); }
};
class PatternFormatter final : public flux::Formatter {
    std::string pat_;
    SymbolTable* n_;
    SymbolTable* f_;

 public:
    PatternFormatter(std::string p, SymbolTable* n, SymbolTable* f) : pat_(std::move(p)), n_(n), f_(f) {}
    void format(const LogItem& i, std::string& o) noexcept override
    {
        try {
            std::ostringstream ts;
            auto tp = std::chrono::system_clock::time_point(std::chrono::nanoseconds(i.ns));
            auto t = std::chrono::system_clock::to_time_t(tp);
            std::tm tm{};
            localtime_r(&t, &tm);
            ts << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
            std::string r = pat_;
            auto sub = [&](std::string_view k, std::string v) {
                size_t p = 0;
                while ((p = r.find(std::string("{") + std::string(k) + "}", p)) != std::string::npos) {
                    r.replace(p, k.size() + 2, v);
                    p += v.size();
                }
            };
            sub("ts", ts.str());
            sub("level", std::string(flux::level_str(i.level)));
            sub("tid", std::to_string(i.tid));
            sub("name", n_ ? std::string(n_->resolve(i.name_id)) : "");
            sub("file", f_ ? std::string(f_->resolve(i.file_id)) : "");
            sub("line", std::to_string(i.line));
            sub("msg", std::string(i.view()));
            o += r;
            o += '\n';
        } catch (...) {
        }
    }
};
struct State {
    std::mutex m;
    bool running = false, stopping = false;
    flux::Config cfg;
    SymbolTable names, files;
    std::unordered_map<std::string, std::unique_ptr<flux::Logger>> loggers;
    flux::detail::mpmc_bounded_queue<LogItem> q;
    std::vector<std::shared_ptr<flux::Sink>> sinks;
    std::unique_ptr<PatternFormatter> fmt;
    std::thread worker;
    flux::Stats st;
    std::condition_variable cv;
};
State& state()
{
    static State s;
    return s;
}
void backend()
{
    auto& s = state();
    for (;;) {
        LogItem x;
        if (!s.q.wait_pop(x, s.cfg.flush_interval)) {
            std::lock_guard l(s.m);
            if (s.stopping && s.q.size() == 0) break;
            continue;
        }
        std::string line;
        s.fmt->format(x, line);
        std::string_view sv = line;
        for (auto& k : s.sinks) {
            if (static_cast<unsigned>(x.level) < static_cast<unsigned>(k->level())) {
                std::lock_guard l(s.m);
                s.st.sink_filtered++;
                continue;
            }
            if (k->write(std::span<const std::string_view>(&sv, 1))) {
                std::lock_guard l(s.m);
                s.st.persisted[k->name()]++;
            } else {
                std::lock_guard l(s.m);
                s.st.sink_failed++;
            }
        }
        if (x.level >= flux::Level::Error)
            for (auto& k : s.sinks) {
                if (k->flush()) {
                    std::lock_guard l(s.m);
                    s.st.flushes++;
                }
            }
    }
    for (auto& k : s.sinks)
        k->flush();
}
}  // namespace

namespace flux {
std::unique_ptr<Formatter> make_pattern_formatter(std::string_view p, detail::SymbolTable* n, detail::SymbolTable* f)
{
    return std::make_unique<PatternFormatter>(
        p.empty() ? "{ts} [{level}] [{tid}] [{name}] {file}:{line} {msg}" : std::string(p), n, f);
}
std::shared_ptr<Sink> make_file_sink(std::string p, std::optional<Rotate> r, std::size_t b)
{
    return std::make_shared<FileSink>(std::move(p), b, r);
}
std::shared_ptr<Sink> make_rotating_file_sink(std::string p, Rotate r, std::size_t b)
{
    return make_file_sink(std::move(p), r, b);
}
std::shared_ptr<Sink> make_stdout_sink(std::FILE* f)
{
    return std::make_shared<StdoutSink>(f);
}
std::shared_ptr<Sink> make_daily_file_sink(std::string p, unsigned keep, DailyClock c)
{
    return std::make_shared<DailySink>(std::move(p), keep, std::move(c));
}
void init(Config c)
{
    if (c.sinks.empty()) throw std::invalid_argument("at least one sink required");
    if (c.queue_items < 2 || (c.queue_items & (c.queue_items - 1)))
        throw std::invalid_argument("queue_items must be power of two");
    auto& s = state();
    std::lock_guard l(s.m);
    if (s.running) throw std::logic_error("already initialized");
    s.cfg = std::move(c);
    s.q.reset(s.cfg.queue_items);
    s.sinks = s.cfg.sinks;
    s.fmt = std::make_unique<PatternFormatter>(
        s.cfg.pattern.empty() ? "{ts} [{level}] [{tid}] [{name}] {file}:{line} {msg}" : s.cfg.pattern, &s.names,
        &s.files);
    s.stopping = false;
    s.running = true;
    s.worker = std::thread(backend);
}
Logger& get(std::string_view n)
{
    auto& s = state();
    std::lock_guard l(s.m);
    auto it = s.loggers.find(std::string(n));
    if (it != s.loggers.end()) return *it->second;
    auto p = std::make_unique<Logger>();
    p->name_ = n;
    auto& x = *p;
    s.loggers.emplace(p->name_, std::move(p));
    return x;
}
void flush(std::chrono::milliseconds)
{
    auto& s = state();
    std::lock_guard l(s.m);
    for (auto& k : s.sinks)
        k->flush();
    s.st.flushes++;
}
void shutdown(std::chrono::milliseconds timeout)
{
    auto& s = state();
    {
        std::lock_guard l(s.m);
        if (!s.running) return;
        s.stopping = true;
        s.running = false;
    }
    auto until = std::chrono::steady_clock::now() + timeout;
    while (s.q.size() && std::chrono::steady_clock::now() < until)
        std::this_thread::yield();
    if (s.q.size()) {
        LogItem x;
        while (s.q.try_pop(x))
            s.st.shutdown_abandoned++;
    }
    {
        std::lock_guard l(s.m);
        s.q.notify();
    }
    if (s.worker.joinable()) s.worker.join();
}
Stats stats()
{
    auto& s = state();
    std::lock_guard l(s.m);
    return s.st;
}
}  // namespace flux

namespace flux::detail {
void emit(Logger& logger, Level lvl, std::source_location loc, std::string msg) noexcept
{
    try {
        auto& s = state();
        if (msg.size() > 65535) {
            std::lock_guard l(s.m);
            s.st.oversize_truncated++;
            msg.resize(65535);
        }
        LogItem i;
        i.level = lvl;
        i.ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
                   .count();
        i.tid = (uint32_t)::gettid();
        i.name_id = s.names.intern(logger.name_);
        i.file_id = s.files.intern(basename(loc.file_name()));
        i.line = loc.line();
        i.text_len = (uint16_t)msg.size();
        if (msg.size() <= 256)
            std::memcpy(i.text, msg.data(), msg.size());
        else {
            i.spill = std::make_unique<char[]>(msg.size());
            std::memcpy(i.spill.get(), msg.data(), msg.size());
        }
        for (;;) {
            {
                std::lock_guard l(s.m);
                if (!s.running) {
                    if (s.stopping)
                        s.st.post_shutdown++;
                    else
                        s.st.pre_init++;
                    std::fwrite(msg.data(), 1, msg.size(), stderr);
                    std::fputc('\n', stderr);
                    return;
                }
            }
            if (s.q.try_push(std::move(i))) {
                std::lock_guard l(s.m);
                s.st.accepted++;
                return;
            }
            if (logger.policy_ == OverflowPolicy::DropNewest) {
                std::lock_guard l(s.m);
                s.st.queue_dropped++;
                return;
            }
            std::this_thread::yield();
        }
    } catch (...) {
        auto& s = state();
        std::lock_guard l(s.m);
        s.st.format_failed++;
    }
}
}  // namespace flux::detail
