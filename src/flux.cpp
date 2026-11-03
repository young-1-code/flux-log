#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstring>
#include <filesystem>
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
void record_format_failed() noexcept;
// 符号表只在注册阶段加写锁；运行阶段通过稳定指针原子读取。
class SymbolTable {
    static constexpr std::size_t max_symbols = 65536;
    std::unique_ptr<std::atomic<const std::string*>[]> slots_;
    std::vector<std::unique_ptr<std::string>> storage_;
    std::unordered_map<std::string, std::uint16_t> ids_;
    std::mutex write_mutex_;

 public:
    SymbolTable() : slots_(std::make_unique<std::atomic<const std::string*>[]>(max_symbols))
    {
        for (std::size_t i = 0; i < max_symbols; ++i)
            slots_[i].store(nullptr, std::memory_order_relaxed);
    }
    std::uint16_t intern(std::string_view value) noexcept
    {
        try {
            std::lock_guard lock(write_mutex_);
            auto it = ids_.find(std::string(value));
            if (it != ids_.end()) return it->second;
            if (storage_.size() + 1 >= max_symbols) return 0;
            auto text = std::make_unique<std::string>(value);
            const auto id = static_cast<std::uint16_t>(storage_.size() + 1);
            const auto* stable = text.get();
            storage_.push_back(std::move(text));
            ids_.emplace(*stable, id);
            slots_[id].store(stable, std::memory_order_release);
            return id;
        } catch (...) {
            return 0;
        }
    }
    std::string_view resolve(std::uint16_t id) const noexcept
    {
        if (id == 0) return {};
        const auto* value = slots_[id].load(std::memory_order_acquire);
        return value ? std::string_view(*value) : std::string_view{};
    }
};
// 队列元素固定为 288B；spill 只在消息超过 256B 时分配。
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
[[maybe_unused]] std::string basename(std::string_view s)
{
    auto p = s.find_last_of("/\\");
    return std::string(s.substr(p == std::string_view::npos ? 0 : p + 1));
}
// 文件 sink 只由后端线程访问，内部缓冲减少 write 次数。
class FileSink final : public flux::Sink {
 protected:
    std::string path_;
    int fd_{-1};
    std::string buf_;
    size_t limit_;
    std::size_t written_{0};
    std::optional<flux::Rotate> rot_;

 public:
    FileSink(std::string p, size_t b, std::optional<flux::Rotate> r) : path_(std::move(p)), limit_(b), rot_(r)
    {
        fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) throw std::system_error(errno, std::generic_category(), "open log");
        struct stat st {};
        if (::fstat(fd_, &st) == 0) written_ = static_cast<std::size_t>(st.st_size);
    }
    ~FileSink() override
    {
        flush();
        if (fd_ >= 0) ::close(fd_);
    }
    bool write(std::span<const std::string_view> ls) noexcept override
    {
        try {
            size_t n = 0;
            for (auto s : ls)
                n += s.size();
            if (rot_ && rot_->max_size && written_ + buf_.size() + n > rot_->max_size) {
                flush();
                ::close(fd_);
                fd_ = -1;
                for (unsigned i = rot_->keep; i > 0; --i) {
                    auto a = path_ + "." + std::to_string(i), b = path_ + "." + std::to_string(i + 1);
                    if (i == rot_->keep)
                        std::filesystem::remove(a);
                    else if (std::filesystem::exists(a))
                        std::filesystem::rename(a, b);
                }
                if (std::filesystem::exists(path_)) std::filesystem::rename(path_, path_ + ".1");
                fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
                if (fd_ < 0) return false;
                written_ = 0;
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
            std::size_t offset = 0;
            while (offset < buf_.size()) {
                const auto n = ::write(fd_, buf_.data() + offset, buf_.size() - offset);
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) return false;
                offset += static_cast<std::size_t>(n);
            }
            written_ += buf_.size();
            if (fsync_ == flux::FsyncPolicy::Always && ::fsync(fd_) != 0) return false;
            bool ok = true;
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
// PatternFormatter 把生产时间戳和符号表字段渲染成最终文本行。
class PatternFormatter final : public flux::Formatter {
    enum class Kind { Literal, Timestamp, Level, Tid, Name, File, Line, Message };
    struct Token {
        Kind kind;
        std::string literal;
    };
    std::vector<Token> tokens_;
    SymbolTable* names_;
    SymbolTable* files_;

 public:
    PatternFormatter(std::string p, SymbolTable* names, SymbolTable* files) : names_(names), files_(files)
    {
        std::size_t pos = 0;
        while (pos < p.size()) {
            auto open = p.find('{', pos);
            if (open != pos)
                tokens_.push_back(
                    {Kind::Literal, p.substr(pos, open == std::string::npos ? p.size() - pos : open - pos)});
            if (open == std::string::npos) break;
            auto close = p.find('}', open + 1);
            if (close == std::string::npos) throw std::invalid_argument("unterminated pattern token");
            auto key = std::string_view(p).substr(open + 1, close - open - 1);
            Kind kind;
            if (key == "ts")
                kind = Kind::Timestamp;
            else if (key == "level")
                kind = Kind::Level;
            else if (key == "tid")
                kind = Kind::Tid;
            else if (key == "name")
                kind = Kind::Name;
            else if (key == "file")
                kind = Kind::File;
            else if (key == "line")
                kind = Kind::Line;
            else if (key == "msg")
                kind = Kind::Message;
            else
                throw std::invalid_argument("unknown pattern token: " + std::string(key));
            tokens_.push_back({kind, {}});
            pos = close + 1;
        }
    }
    void format(const LogItem& i, std::string& o) noexcept override
    {
        try {
            std::string timestamp;
            for (const auto& token : tokens_) {
                switch (token.kind) {
                    case Kind::Literal:
                        o += token.literal;
                        break;
                    case Kind::Timestamp:
                        if (timestamp.empty()) {
                            std::ostringstream ts;
                            auto tp = std::chrono::system_clock::time_point(std::chrono::nanoseconds(i.ns));
                            auto t = std::chrono::system_clock::to_time_t(tp);
                            std::tm tm{};
                            localtime_r(&t, &tm);
                            const auto micros =
                                std::chrono::duration_cast<std::chrono::microseconds>(tp.time_since_epoch()) % 1000000;
                            ts << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0') << std::setw(6)
                               << micros.count();
                            timestamp = ts.str();
                        }
                        o += timestamp;
                        break;
                    case Kind::Level:
                        o += flux::level_str(i.level);
                        break;
                    case Kind::Tid:
                        o += std::to_string(i.tid);
                        break;
                    case Kind::Name:
                        if (names_) o += names_->resolve(i.name_id);
                        break;
                    case Kind::File:
                        if (files_) o += files_->resolve(i.file_id);
                        break;
                    case Kind::Line:
                        o += std::to_string(i.line);
                        break;
                    case Kind::Message:
                        o += i.view();
                        break;
                }
            }
            o += '\n';
        } catch (...) {
        }
    }
};
// 默认 pattern 的专用快速路径：字段顺序固定，不再解释 token。
class FastDefaultFormatter final : public flux::Formatter {
    SymbolTable* names_;
    SymbolTable* files_;
    static void append_uint(std::string& out, std::uint64_t value)
    {
        char buffer[32];
        auto [end, error] = std::to_chars(buffer, buffer + sizeof(buffer), value);
        if (error == std::errc{}) out.append(buffer, end);
    }
    static void append_timestamp(std::string& out, std::uint64_t ns)
    {
        const auto tp = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ns));
        const auto seconds = std::chrono::system_clock::to_time_t(tp);
        std::tm tm{};
        localtime_r(&seconds, &tm);
        char date[32];
        const auto length = std::strftime(date, sizeof(date), "%Y-%m-%d %H:%M:%S", &tm);
        out.append(date, length);
        out.push_back('.');
        const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(tp.time_since_epoch()) % 1000000;
        char digits[16];
        auto [end, error] = std::to_chars(digits, digits + sizeof(digits), micros.count());
        if (error == std::errc{}) {
            const auto count = static_cast<std::size_t>(end - digits);
            out.append(6 - count, '0');
            out.append(digits, end);
        }
    }

 public:
    FastDefaultFormatter(SymbolTable* names, SymbolTable* files) : names_(names), files_(files) {}
    void format(const LogItem& item, std::string& out) noexcept override
    {
        try {
            out.reserve(out.size() + 64 + item.text_len);
            append_timestamp(out, item.ns);
            out.append(" [");
            out.append(flux::level_str(item.level));
            out.append("][");
            append_uint(out, item.tid);
            out.append("][");
            out.append(names_->resolve(item.name_id));
            out.append("][");
            out.append(files_->resolve(item.file_id));
            out.push_back(':');
            append_uint(out, item.line);
            out.append("]");
            out.append(item.view());
            out.push_back('\n');
        } catch (...) {
        }
    }
};
// 全局状态由 registry 持有；s.m 保护生命周期、logger 表和统计数据。
struct State {
    std::mutex m;
    std::atomic_bool running{false}, stopping{false};
    flux::Config cfg;
    SymbolTable names, files;
    std::unordered_map<std::string, std::unique_ptr<flux::Logger>> loggers;
    flux::detail::mpmc_bounded_queue<LogItem> q;
    std::vector<std::shared_ptr<flux::Sink>> sinks;
    std::unique_ptr<flux::Formatter> fmt;
    std::thread worker;
    struct AtomicStats {
        std::atomic_uint64_t accepted{0}, queue_dropped{0}, shutdown_abandoned{0}, format_failed{0};
        std::atomic_uint64_t oversize_truncated{0}, metadata_truncated{0}, sink_filtered{0}, sink_failed{0};
        std::atomic_uint64_t pre_init{0}, post_shutdown{0}, flushes{0};
    } at;
    std::unique_ptr<std::atomic_uint64_t[]> persisted;
    std::size_t persisted_size{0};
    std::condition_variable cv;
};
State& state()
{
    static State s;
    return s;
}
// 后端循环：阻塞取一条，向每个满足等级阈值的 sink 写入，再按需 flush。
void backend()
{
    auto& s = state();
    constexpr std::size_t batch_capacity = 256;
    std::vector<LogItem> batch;
    std::vector<std::string> lines;
    std::vector<std::string_view> views;
    batch.reserve(batch_capacity);
    lines.resize(batch_capacity);
    views.reserve(batch_capacity);
    for (;;) {
        LogItem first;
        if (!s.q.wait_pop(first, s.cfg.flush_interval)) {
            std::lock_guard l(s.m);
            if (s.stopping && s.q.size() == 0) break;
            continue;
        }
        batch.clear();
        batch.push_back(std::move(first));
        while (batch.size() < batch_capacity) {
            LogItem item;
            if (!s.q.try_pop(item)) break;
            batch.push_back(std::move(item));
        }
        for (std::size_t i = 0; i < batch.size(); ++i) {
            // 保留 string 的内部容量，只清除上一批的内容。
            lines[i].clear();
            s.fmt->format(batch[i], lines[i]);
        }
        bool flush_needed = false;
        for (const auto& item : batch)
            if (item.level >= flux::Level::Error) flush_needed = true;
        for (std::size_t si = 0; si < s.sinks.size(); ++si) {
            auto& k = s.sinks[si];
            views.clear();
            for (std::size_t i = 0; i < batch.size(); ++i) {
                if (static_cast<unsigned>(batch[i].level) < static_cast<unsigned>(k->level())) {
                    s.at.sink_filtered++;
                } else {
                    views.emplace_back(lines[i]);
                }
            }
            if (views.empty()) continue;
            if (k->write(views)) {
                s.persisted[si].fetch_add(views.size(), std::memory_order_relaxed);
            } else {
                s.at.sink_failed.fetch_add(views.size(), std::memory_order_relaxed);
            }
            if (flush_needed && k->flush()) s.at.flushes++;
        }
    }
    // 退出时最后一次 flush，正常批次只在周期或 error/panic 时 flush。
    for (auto& k : s.sinks)
        k->flush();
}
}  // namespace

namespace flux {
std::unique_ptr<Formatter> make_pattern_formatter(std::string_view p, detail::SymbolTable* names,
                                                  detail::SymbolTable* files)
{
    return std::make_unique<PatternFormatter>(
        p.empty() ? "{ts} [{level}][{tid}][{name}][{file}:{line}]{msg}" : std::string(p), names, files);
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
    s.persisted_size = s.sinks.size();
    s.persisted = std::make_unique<std::atomic_uint64_t[]>(s.persisted_size);
    for (std::size_t i = 0; i < s.persisted_size; ++i)
        s.persisted[i].store(0);
    if (s.cfg.pattern.empty())
        s.fmt = std::make_unique<FastDefaultFormatter>(&s.names, &s.files);
    else
        s.fmt = std::make_unique<PatternFormatter>(s.cfg.pattern, &s.names, &s.files);
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
    p->name_id_ = s.names.intern(p->name_);
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
    s.at.flushes++;
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
            s.at.shutdown_abandoned++;
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
    Stats out;
    out.accepted = s.at.accepted.load();
    out.queue_dropped = s.at.queue_dropped.load();
    out.shutdown_abandoned = s.at.shutdown_abandoned.load();
    out.format_failed = s.at.format_failed.load();
    out.oversize_truncated = s.at.oversize_truncated.load();
    out.metadata_truncated = s.at.metadata_truncated.load();
    out.sink_filtered = s.at.sink_filtered.load();
    out.sink_failed = s.at.sink_failed.load();
    out.pre_init = s.at.pre_init.load();
    out.post_shutdown = s.at.post_shutdown.load();
    out.flushes = s.at.flushes.load();
    for (std::size_t i = 0; i < s.sinks.size(); ++i)
        out.persisted[s.sinks[i]->name()] = s.persisted[i].load();
    return out;
}
}  // namespace flux

namespace flux::detail {
void record_format_failed() noexcept
{
    auto& s = state();
    s.at.format_failed++;
}

void emit(Logger& logger, Level lvl, std::source_location loc, std::string msg) noexcept
{
    try {
        auto& s = state();
        if (msg.size() > 65535) {
            s.at.oversize_truncated++;
            msg.resize(65535);
        }
        LogItem i;
        i.level = lvl;
        i.ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
                   .count();
        thread_local std::uint32_t cached_tid = static_cast<std::uint32_t>(::gettid());
        i.tid = cached_tid;
        i.name_id = logger.name_id_;
        thread_local const char* cached_file = nullptr;
        thread_local std::uint16_t cached_file_id = 0;
        if (loc.file_name() != cached_file) {
            cached_file = loc.file_name();
            cached_file_id = s.files.intern(basename(loc.file_name()));
        }
        i.file_id = cached_file_id;
        i.line = loc.line();
        i.text_len = (uint16_t)msg.size();
        if (msg.size() <= 256)
            std::memcpy(i.text, msg.data(), msg.size());
        else {
            i.spill = std::make_unique<char[]>(msg.size());
            std::memcpy(i.spill.get(), msg.data(), msg.size());
        }
        for (;;) {
            if (!s.running.load(std::memory_order_acquire)) {
                if (s.stopping.load(std::memory_order_acquire))
                    s.at.post_shutdown++;
                else
                    s.at.pre_init++;
                std::fwrite(msg.data(), 1, msg.size(), stderr);
                std::fputc('\n', stderr);
                return;
            }
            if (s.q.try_push(std::move(i))) {
                s.at.accepted++;
                return;
            }
            if (logger.policy_ == OverflowPolicy::DropNewest) {
                s.at.queue_dropped++;
                return;
            }
            std::this_thread::yield();
        }
    } catch (...) {
        auto& s = state();
        s.at.format_failed++;
    }
}
}  // namespace flux::detail
