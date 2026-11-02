# flux-log 异步日志组件实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 `docs/flux-log-design.md` 定义的 C++20 高性能异步日志组件（覆盖 M1+M2：核心链路 + 生产化；M3 明确排除）。

**Architecture:** 业务线程"级别过滤→取时间戳→参数格式化→MPMC 入队"，单后端 `jthread` 批量出队、行组装、按 sink 批量落盘；注册表 append-only 符号表（logger 名/文件名驻留），显式 init/shutdown 生命周期。

**Tech Stack:** C++20（仅标准库）、CMake ≥3.24、GTest + google-benchmark（FetchContent，可关）、Linux（pthread 绑核）。

**Spec:** `docs/flux-log-design.md`（所有决策以 spec 为准；本计划与 spec §3.2 的一处偏差见 Global Constraints 末条）

## Global Constraints

- C++20；gcc ≥ 13 或 clang ≥ 16（`std::format` 门槛）；编译选项 `-Wall -Wextra -Wpedantic`
- 库代码**仅标准库**；GTest/google-benchmark 仅存在于 `tests/`、`bench/`，由 `FLUX_BUILD_TESTS`(默认 ON)、`FLUX_BUILD_BENCH`(默认 OFF) 控制，FetchContent 拉取
- 命名空间 `flux`（公开）/ `flux::detail`（内部）；产物为静态库 + CMake install；构建：`cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j`；测试：`ctest --test-dir build --output-on-failure`
- 所有公开日志入口 `noexcept`；热路径（`Logger::log` 及宏展开）无锁、无堆分配（超长消息 spill 除外）
- `LogItem` 定长 **288 字节**（`static_assert` 钉死）；内联文本 256B；消息上限 65535B（超出截断并计数）；>256B 走 `spill`
- 默认值（spec §5）：`queue_items=16384`（2 的幂，否则 `init` 抛 `std::invalid_argument`）、`overflow=DropNewest`、`flush_interval=200ms`、每个 sink 默认 `fsync=OnRotation`、FileSink 写缓冲 1MiB、`pin_backend=false`；`sinks` 不能为空
- 时间戳：本地时区 `%Y-%m-%d %H:%M:%S.ffffff`（6 位微秒）；主机无 tzdata 时回退 UTC 并 stderr 告警一次
- 默认 pattern：`{ts} [{level}][{tid}][{name}][{file}:{line}]{msg}`；级别串小写 `trace/debug/info/warn/error/panic`；`{file}` 只渲染**文件 basename**；行由 Formatter 追加 `\n`
- Sink 写失败语义：`Sink::write` 返回 `bool`（绝不抛）；后端连续 3 次失败熔断 10s（Backend 构造参数可注入，供测试）
- 原始根目录 `mpmc_queue.hpp` 已迁移为 `src/detail/mpmc_queue.hpp` 的无锁实现；根目录旧文件删除，避免两份队列实现并存
- 掉落汇总：DropNewest 丢消息时后端每秒最多一条 stderr 汇总（文案含当期/累计数）
- 新建 Logger 默认级别为 `Info`；未知枚举、非法容量、负时间和空 sinks 均拒绝 `init()`，不静默回退
- shutdown 发布停止标志后拒绝新日志，已进入提交阶段的日志允许完成；超时放弃剩余队列并计入 `shutdown_abandoned`
- 格式化/分配失败丢弃当前日志并计数；所有内部告警绕过 flux 队列，直接写 stderr 并限频
- 提交纪律：每任务一提交，消息 `feat|fix|test|chore: <一行摘要>`；不要 push
- `LogItem` 携带 `file_id`(u16，符号表驻留 basename) + `line`(u32)，保住 `{file}{line}` 独立占位符且生产端零额外格式化成本；总大小按 256B 内联文本固定为 288B
- spec §6 特性落点：`jthread/stop_token`=Task 7、`source_location/std::format`=Task 8、`concepts`=Task 4（Formatter token 处理的模板约束）、`span`=Task 5/7、designated initializers=Task 1、`consteval`=Task 4（默认 pattern 校验可用则用，不强求）

## Review Focus

规格未逐条点名、但最容易坑到使用者的五类输入/工况（每条已钉到归属任务的测试）：

1. **突发打满队列**：分别核对 `accepted`、`queue_dropped`、`shutdown_abandoned` 与各 sink `persisted`；Block 策略在 shutdown 时必须能脱身不死锁 → Task 6（队列层）、Task 12（端到端守恒）
2. **超长消息**（>256B spill、>65535B 截断）：行内容完整、跨线程无泄漏 → Task 3（单元）、Task 12（端到端全文比对）
3. **生命周期边界**：init 前日志（stderr 兜底 + 一次性告警 + 计数）、shutdown 后拒绝且计数、init→shutdown→init 多轮、并发同名 `get()` 单实例 → Task 8
4. **sink 故障隔离**：单个 sink 失败不炸后端、错误计数、熔断期间其余 sink 正常、`shutdown(timeout)` 不挂死 → Task 11
5. **并发正确性**：多线程生产/消费（TSAN 干净）、同名并发注册唯一性 → Task 12（TSAN/ASAN 跑全部测试）

---

### Task 1: 工程脚手架 + Level/Config 类型

**Files:**
- Create: `CMakeLists.txt`, `include/flux/log.hpp`, `include/flux/level.hpp`, `include/flux/config.hpp`, `tests/CMakeLists.txt`, `tests/test_config.cpp`, `.gitignore` 追加 `build/`

**Interfaces:**
- Produces（后续所有任务依赖）:
  - `enum class Level : std::uint8_t { Trace, Debug, Info, Warn, Error, Critical, Off };`
  - `constexpr std::string_view level_str(Level) noexcept;`（返回小写串，未知返回 `"off"`）
  - `enum class OverflowPolicy : std::uint8_t { DropNewest, Block };`、`enum class FsyncPolicy : std::uint8_t { Never, OnRotation, Always };`
  - `struct Rotate { std::size_t max_size; unsigned keep; };`
  - `struct Config { std::size_t queue_items=16384; OverflowPolicy overflow=OverflowPolicy::DropNewest; std::chrono::milliseconds flush_interval{200}; bool pin_backend=false; std::vector<std::shared_ptr<Sink>> sinks; std::string pattern; };`（`sinks` 非空；每个 sink 独立配置 fsync）
  - `struct Stats { std::uint64_t accepted=0, queue_dropped=0, shutdown_abandoned=0, format_failed=0, oversize_truncated=0, sink_filtered=0, sink_failed=0, pre_init=0, post_shutdown=0, flushes=0; };`（另按 sink 统计 persisted）
  - CMake 目标：库 `flux`（暂为空实现）、`FLUX_BUILD_TESTS/FLUX_BUILD_BENCH/FLUX_SANITIZE(none|asan|tsan)` 选项

- [ ] **Step 1: 写失败测试** `tests/test_config.cpp`

```cpp
#include <flux/config.hpp>
#include <flux/level.hpp>
#include <gtest/gtest.h>
TEST(Level, OrderAndStrings) {
    EXPECT_TRUE(Level::Trace < Level::Debug && Level::Critical < Level::Off);
    EXPECT_EQ(flux::level_str(Level::Info), "info");
    EXPECT_EQ(flux::level_str(Level::Warn), "warn");
}
TEST(Config, DefaultsMatchSpec) {
    flux::Config c;
    EXPECT_EQ(c.queue_items, 16384u);
    EXPECT_EQ(c.overflow, flux::OverflowPolicy::DropNewest);
    EXPECT_EQ(c.flush_interval, std::chrono::milliseconds(200));
    EXPECT_EQ(c.fsync, flux::FsyncPolicy::OnRotation);
    EXPECT_FALSE(c.pin_backend);
    EXPECT_TRUE(c.sinks.empty()); EXPECT_TRUE(c.pattern.empty());
}
```

- [ ] **Step 2: 构建并确认失败** — `cmake -S . -B build -DFLUX_BUILD_TESTS=ON && cmake --build build -j && ctest --test-dir build -R config --output-on-failure`，期望：编译错误（头文件不存在）。同时验证 `g++ --version` ≥ 13，否则报告并停止
- [ ] **Step 3: 实现** — 顶层 `CMakeLists.txt`（C++20、选项、`add_subdirectory(tests)`、空的 `add_library(flux INTERFACE)` 暂可，Task 2 起改 STATIC 并加源文件）、`tests/CMakeLists.txt`（FetchContent GTest、每测试文件一可执行 `flux_test_<名>`、自动 `discover_tests`）、三个头文件（`log.hpp` 先只 include level/config，后续任务扩充）
- [ ] **Step 4: 跑测试通过** — 同 Step 2 命令，期望 PASS
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: cmake scaffold, Level and Config types"`

### Task 2: 现代化 MPMC 队列

**Files:**
- Create: `src/detail/mpmc_queue.hpp`, `tests/test_mpmc.cpp`
- Modify: `CMakeLists.txt`（flux 转 STATIC，装 `src/detail` 路径）、`tests/CMakeLists.txt`

**Interfaces:**
  - Produces: `flux::detail::mpmc_bounded_queue<T>`：`explicit mpmc_bounded_queue(size_t n)`（n 非 2 的幂或 <2 抛 `std::invalid_argument`，**校验先于分配**）；`bool try_push(T&&)` / `bool try_pop(T&)`；`size_t size() const noexcept`（= enqueue−dequeue，无符号回绕天然正确）；`size_t capacity() const noexcept`；默认构造 = 容量 0、所有成员初始化（size()/capacity() 不 UB）；`std::unique_ptr<cell_t[]>` 持有；cacheline 对齐用 `std::hardware_destructive_interference_size`（无值时回退 64）。实现基于 Vyukov 算法并统一位于 `src/detail`。

- [ ] **Step 1: 写失败测试** `tests/test_mpmc.cpp`

```cpp
TEST(Mpmc, RejectsNonPowerOfTwo) {
    EXPECT_THROW(flux::detail::mpmc_bounded_queue<int> q(3), std::invalid_argument);
    EXPECT_THROW(flux::detail::mpmc_bounded_queue<int> q(0), std::invalid_argument);
    EXPECT_NO_THROW([]{ flux::detail::mpmc_bounded_queue<int> q(2); }());
}
TEST(Mpmc, DefaultCtorIsSafe) {
    flux::detail::mpmc_bounded_queue<int> q;      // 不 UB
    EXPECT_EQ(q.capacity(), 0u); EXPECT_EQ(q.size(), 0u);
    int v; EXPECT_FALSE(q.try_pop(v));
}
TEST(Mpmc, SpscFifo) { /* 容量16，push 1000/pop 1000 交替，值序一致，size 终态 0 */ }
TEST(Mpmc, MpmcConservation) { /* 4 生产×4 消费，各 push/pop 25000，校验 sum 与 multiset 守恒 */ }
```

- [ ] **Step 2: 跑测试确认失败**（头文件不存在，编译失败）
- [ ] **Step 3: 实现** — 按 Interfaces 迁移改造；删除 `init()`；异常类型换 `std::invalid_argument`
- [ ] **Step 4: 跑测试通过**（含 MPMC 守恒）
- [ ] **Step 5: Commit** — `feat: modernized Vyukov MPMC queue (fixes spec §11)`

### Task 3: LogItem + 符号表

**Files:**
- Create: `src/log_item.hpp`, `src/symbol_table.hpp`, `tests/test_log_item.cpp`

**Interfaces:**
- Produces:
  - `flux::detail::SymbolTable`：固定 65536 槽 `std::vector<std::atomic<const std::string*>>`（构造时一次预分配并置空）；`std::uint16_t intern(std::string_view) noexcept`（写者互斥；超限/重复见下）；`std::string_view resolve(std::uint16_t) const noexcept`（读者无锁，acquire load；空槽返回 `""`）。同串返回同 id；槽满后返回固定符号 `"(sym-overflow)"` 的 id。程序生命周期 append-only，字符串永不释放（故意的）
  - `flux::detail::LogItem`：

```cpp
struct LogItem {
    std::uint64_t ns;        // realtime 纳秒（生产者采集）
    std::uint32_t tid;       // thread_local 缓存 gettid()
    std::uint16_t text_len;  // 消息长度（≤65535；>256 表示走 spill）
    Level         level;
    std::uint16_t name_id;   // logger 名符号
    std::uint16_t file_id;   // 文件 basename 符号
    std::uint32_t line;
    char text[256];
    std::unique_ptr<char[]> spill;   // text_len>256 时有效，长度=text_len
    std::string_view view() const noexcept;  // text 或 spill 的视图
};
static_assert(sizeof(LogItem) == 288);
```

- [ ] **Step 1: 写失败测试**：`EXPECT_EQ(sizeof(flux::detail::LogItem), 288u)`；SymbolTable：`intern("a")==intern("a")`、`intern("a")!=intern("b")`、`resolve(intern("x"))=="x"`；手工构造 spill 消息（len=300）断言 `view()` 全长正确
- [ ] **Step 2: 确认失败** → **Step 3: 实现**（`view()` 返回 `{spill.get(), text_len}` 或 `{text, text_len}`）
- [ ] **Step 4: 通过** → **Step 5: Commit** `feat: LogItem and lock-free-read symbol table`

### Task 4: PatternFormatter

**Files:**
- Create: `include/flux/formatter.hpp`, `src/formatter.cpp`, `tests/test_formatter.cpp`

**Interfaces:**
- Consumes: Task 3 的 `LogItem`/`SymbolTable`
- Produces:
  - `class Formatter { public: virtual ~Formatter()=default; virtual void format(const detail::LogItem&, std::string& out) noexcept = 0; };`（追加整行含 `\n`）
  - `std::unique_ptr<Formatter> flux::make_pattern_formatter(std::string_view pattern);`（空串=默认 pattern；未知 token 抛 `std::invalid_argument`——init 期异常合法，非热路径）

- [ ] **Step 1: 写失败测试** `tests/test_formatter.cpp`

```cpp
// 构造 LogItem{ns=固定值, tid=4242, level=Info, text="hello", line=77}
// names.intern("order") / files.intern("order.cpp") 预备符号表
TEST(Formatter, SimplePatternExact) {
    auto f = flux::make_pattern_formatter("[{level}][{name}]{msg}");
    std::string out; f->format(item, out);
    EXPECT_EQ(out, "[info][order]hello\n");
}
TEST(Formatter, FileTokenIsBasename) {
    // files.intern 传入 "/a/b/order.cpp" → intern 存 basename "order.cpp"
    auto f = flux::make_pattern_formatter("{file}:{line}");
    ... EXPECT_EQ(out, "order.cpp:77\n");
}
TEST(Formatter, DefaultPatternContainsTokens) {
    // 默认 pattern 输出按序包含 ts、"info"、"4242"、"order"、"order.cpp:77"、"hello"
}
TEST(Formatter, TimestampRoundsTrip) {
    // {ts} 渲染结果 parse 回 timepoint（getlocale/parse "%Y-%m-%d %H:%M:%S.%6f"）与 ns 相差 ≤1µs；时区=本地
}
TEST(Formatter, UnknownTokenThrows) {
    EXPECT_THROW(flux::make_pattern_formatter("{bogus}"), std::invalid_argument);
}
```

- [ ] **Step 2: 确认失败** → **Step 3: 实现** — init 时解析 pattern 为 token 数组（literal/text、ts、level、tid、name、file、line、msg）；`{ts}` 用 `std::chrono::zoned_time(get_tz(), sys_time)`，`get_tz()` 失败回退 UTC 并 `std::atomic_flag` 保证一次性 stderr 告警
- [ ] **Step 4: 通过** → **Step 5: Commit** `feat: pattern formatter with spec tokens`

### Task 5: Sink 接口 + FileSink + StdoutSink

**Files:**
- Create: `include/flux/sink.hpp`, `src/sinks/file_sink.cpp`, `tests/test_sinks.cpp`

**Interfaces:**
- Consumes: Level（Task 1）
- Produces:
  - `class Sink { public: virtual ~Sink()=default; virtual bool write(std::span<const std::string_view> lines) noexcept = 0; virtual bool flush() noexcept = 0; virtual void set_fsync(FsyncPolicy) noexcept {} Level level() const noexcept; void set_level(Level) noexcept; };`（级别为 `std::atomic<Level>`，后端按它过滤行）
  - `std::shared_ptr<Sink> make_file_sink(std::string path, std::optional<Rotate> rotate = std::nullopt, std::size_t buffer_size = 1<<20);`
  - `std::shared_ptr<Sink> make_stdout_sink(std::FILE* out = stdout);`（参数供测试注入 tmpfile）
  - FileSink 语义：write 追加内部缓冲，**缓冲满自动 flush**；flush = `::write` 全量（循环写）+ 按 `FsyncPolicy::Always` 时 `fsync`；构造时打开/创建文件（失败抛 `std::system_error`——init 期合法）

- [ ] **Step 1: 写失败测试**：FileSink(tmp 路径, buffer=64)：write 一行 10B → 文件仍空；累计 >64B → 文件非空（缓冲满自动落）；`flush()` 后文件含全部行且顺序一致；`set_level(Warn)` 后（后端过滤逻辑在 Task 7，此处只测存取器）；StdoutSink：注入 `std::tmpfile`，write+flush 后 `std::fseek/fread` 读回内容；返回值：正常 `true`
- [ ] **Step 2: 确认失败** → **Step 3: 实现**（`::open(O_WRONLY|O_CREAT|O_APPEND,0644)`，析构 flush；RAII fd）
- [ ] **Step 4: 通过** → **Step 5: Commit** `feat: Sink interface, FileSink and StdoutSink`

### Task 6: BackendQueue（溢出/唤醒/停机脱身）

**Files:**
- Create: `src/backend_queue.hpp`, `src/backend_queue.cpp`, `tests/test_backend_queue.cpp`

**Interfaces:**
- Consumes: `mpmc_bounded_queue<LogItem>`（Task 2）、`LogItem`（Task 3）
- Produces:

```cpp
namespace flux::detail {
class BackendQueue {
public:
    BackendQueue(std::size_t items_pow2);          // 非 2 的幂转抛 invalid_argument
    bool try_push(LogItem&&) noexcept;             // 满即 false（DropNewest 语义）
    bool blocking_push(LogItem&&) noexcept;        // Block 语义；唯一返回 false 的情形 = 已 request_stop
    std::size_t pop_batch(std::span<LogItem> out) noexcept;   // ≤out.size()，FIFO
    void wait_for_items(std::chrono::milliseconds timeout, std::stop_token st);
                                                   // 唤醒条件：有新项 / stop / 超时
    void request_stop() noexcept;                  // 唤醒 waiter，并让 blocking_push 脱身
    std::size_t capacity() const noexcept;
};
}
```

实现要点：`pending` 原子计数 + `std::condition_variable` + 互斥量只保护 cv 本身；push 成功后 `pending.fetch_add(1, release)` 且仅当后端可能睡眠（`sleeping` 标志）才 `notify_one`；pop_batch 递减 pending。

- [ ] **Step 1: 写失败测试**：容量 16：填满后 `try_push`==false、pop 1 个后成功；FIFO 顺序；`pop_batch(span{8})` 一批 ≤8；**Block 脱身**：填满队列 → `request_stop()` → `blocking_push` 在短时限内返回 false（用 `std::jthread` 计时断言 <1s）；**唤醒**：`wait_for_items(1000ms, stop)` 在另一线程 push 后 ≤100ms 返回
- [ ] **Step 2: 确认失败** → **Step 3: 实现** → **Step 4: 通过** → **Step 5: Commit** `feat: BackendQueue with overflow policies and shutdown escape`

### Task 7: Backend 线程（批量消费/flush/落盘）

**Files:**
- Create: `src/backend.hpp`, `src/backend.cpp`, `tests/test_backend.cpp`

**Interfaces:**
- Consumes: BackendQueue（Task 6）、Formatter（Task 4）、Sink（Task 5）、SymbolTable（Task 3）
- Produces:

```cpp
namespace flux::detail {
class Backend {
public:
    Backend(Config cfg, std::vector<std::shared_ptr<Sink>> sinks,
            std::unique_ptr<Formatter> fmt, SymbolTable& names, SymbolTable& files);
    void start();                                  // 起 jthread
    bool submit(OverflowPolicy pol, LogItem&&) noexcept;
                                                   // pol=DropNewest：队列满即计数 queue_dropped 并返回
                                                   // pol=Block：blocking_push；accepted 在此统一计数
    void flush_sync(std::chrono::milliseconds timeout);  // 显式 flush，等后端 ack
    void shutdown(std::chrono::milliseconds timeout);    // 停收 → 排空 → flush → join，超时放弃剩余队列并计数
    Stats stats() const noexcept;                  // 原子读各计数
    bool running() const noexcept;
};
}
```

后端主循环（一次迭代）：`pop_batch(span 256)` → 逐条 `fmt->format(item, line_buf)`（`line_buf` 只追加，行地址稳定）→ 对每个 sink（item.level ≥ sink.level）收集 `std::string_view` → `sink->write(span)`（返回 false 计 `sink_errors`）→ 若批内 level ≥ Error 或距上次 flush ≥ flush_interval → 全 sink flush + `flushes++` → DropNewest 丢落每秒至多一条 stderr 汇总（`maybe_print_drop_summary` 独立函数可单测）→ 队列空则 `wait_for_items(flush_interval 余量, stop)`。`pin_backend=true` 时 `pthread_setaffinity_np` 绑 CPU0。

- [ ] **Step 1: 写失败测试**：真实线程端到端——Backend(Config{queue_items=64, flush_interval=10ms}, {file_sink}, formatter, 表)；start；submit 3 条（name "order"）→ `shutdown(2s)` → 文件恰 3 行、内容含 "hello"；`flush_sync` 后 `stats().flushes ≥ 1`；submit Error 级 → flushes 自增（无需等周期）；`maybe_print_drop_summary`：同秒第二次调用返回 false/不输出
- [ ] **Step 2: 确认失败** → **Step 3: 实现** → **Step 4: 通过** → **Step 5: Commit** `feat: backend thread with batched drain and flush triggers`

### Task 8: Registry + Logger 热路径 + 宏 + 生命周期

**Files:**
- Create: `include/flux/logger.hpp`, `include/flux/macros.hpp`, `include/flux/registry.hpp`, `src/registry.cpp`, `tests/test_registry.cpp`, `tests/test_compile_strip.cpp`（独立 TU，`FLUX_LOG_COMPILE_LEVEL=3` 编译）

**Interfaces:**
- Consumes: 全部前序
- Produces（公开 API，与 spec §5 一致）:

```cpp
namespace flux {
void init(Config cfg);          // sinks 不能为空；pattern、queue_items、时间和枚举非法均抛配置错误
Logger& get(std::string_view name);   // 首次创建即注册；并发同名返回同一实例
void flush(std::chrono::milliseconds timeout = 1s);
void shutdown(std::chrono::milliseconds timeout = 5s); // 超时放弃剩余队列并计数
Stats stats();
}
class Logger {
public:
    bool should_log(Level lvl) const noexcept;          // relaxed 原子读
    void set_level(Level) noexcept;
    void set_overflow(OverflowPolicy) noexcept;
    template <class... Args>
    void log(Level lvl, std::source_location loc,
             std::format_string<Args...> fmt, Args&&... args) noexcept;
};
```

`Logger::log`（头文件内联）流程：should_log 失败即返回 → 取 `std::chrono::system_clock::now()` ns、TLS 缓存 tid → `std::format_to_n` 进 `thread_local LogItem staging`（尺寸 65535；n>256 时 spill 分配；超过 65535 截断并计数）→ intern name/file（TLS 一级缓存：`last_file_ptr→file_id`）→ `backend_->submit(policy_, std::move(staging))`（staging 的 spill 转移后置空复用）。Backend 指针取自 registry 的原子快照；为空 = **pre-init 兜底**：默认 formatter 同步渲染整行写 stderr + 一次性告警并计数；shutdown 后拒绝并计数。格式化或分配失败丢弃当前日志、计数并限频告警。

**未调 shutdown 的兜底（spec §8）**：registry 的函数内静态 `unique_ptr` 析构时 best-effort 调 `Backend::shutdown(1s)` 并 stderr 打一条警告。

宏（spec §5 已钉）：`FLUX_LOG` + `FLUX_TRACE/DEBUG/INFO/WARN/ERROR/CRITICAL`。

- [ ] **Step 1: 写失败测试**：init(FileSink(tmp), 默认 pattern) → `FLUX_INFO(order, "oid={} px={}", 42, 1.5)` → shutdown → 文件恰含 `oid=42 px=1.5` 行且带 `order.cpp` basename 与行号（用固定测试文件行）；**pre-init**：不 init 直接 `get("x")` + FLUX_WARN → dup2 捕获 stderr 含告警一次 + 第二条不再告警 + `stats().pre_init==2`；**post-shutdown**：shutdown 后 FLUX_INFO 不落文件、`post_shutdown==1`；**re-init**：完整两轮 init→log→shutdown，两文件各自完整；**并发 get**：8 线程同时 `get("same")` 地址唯一；**未调 shutdown 的兜底**：`fork()` 子进程 init+log 后直接 `_exit` 前 let 静态析构跑（用 `exit(0)` 非 `_exit`）→ 父进程等子退出后断言文件有该行且 stderr 有警告（`#if !defined(__SANITIZE_THREAD__)` 守护，TSAN 与 fork 冲突）；**compile strip**：`test_compile_strip.cpp` 以 `-DFLUX_LOG_COMPILE_LEVEL=3` 编译，设 level=Trace 仍无 Trace/Debug 输出（Info 有）
- [ ] **Step 2: 确认失败** → **Step 3: 实现**（registry 静态存储用 `std::unique_ptr` + 函数内静态，规避静态序问题；`get` 创建路径加锁，热路径无锁）
- [ ] **Step 4: 通过** → **Step 5: Commit** `feat: logger hot path, macros, registry lifecycle`

### Task 9: RotatingFileSink

**Files:**
- Create: `src/sinks/rotating_file_sink.cpp`（接口进 Task 5 的 sink.hpp 工厂声明）, `tests/test_rotating.cpp`

**Interfaces:**
- Consumes: Sink/Rotate/FsyncPolicy
- Produces: `std::shared_ptr<Sink> make_rotating_file_sink(std::string path, Rotate rot, std::size_t buffer_size = 1<<20);`
  语义：写入前检查"已写字节 + 本批"是否 > `max_size` → 轮转：`path` → `path.1`、`path.1`→`path.2`、…、删除 `path.{keep}`；轮转时按 `fsync==OnRotation` fsync 旧文件；重命名失败按 write 失败返回 false

- [ ] **Step 1: 写失败测试**：tmp 下 max_size=100、keep=2：写 3×80B 行（多次 write+flush）→ 存在 `t.log`、`t.1.log`、`t.2.log`，无 `t.3.log`；各文件内容不丢行（总行数守恒）；恰等于 max_size 不轮转（>才轮转）
- [ ] **Step 2: 确认失败** → **Step 3: 实现** → **Step 4: 通过** → **Step 5: Commit** `feat: size-based rotating file sink`

### Task 10: DailyFileSink

**Files:**
- Create: `src/sinks/daily_file_sink.cpp`, `tests/test_daily.cpp`

**Interfaces:**
- Produces: `std::shared_ptr<Sink> make_daily_file_sink(std::string path, unsigned keep, DailyClock clock = nullptr);`
  - `DailyClock = std::function<std::chrono::system_clock::time_point()>`（默认 `system_clock::now`；注入供测试）
  - 文件名：path 含 `{d}` 占位 → 替换为本地日期 `%Y-%m-%d`；否则插入扩展名前（`app.log` → `app.2026-11-01.log`）
  - 语义：每次 write 前比对当前本地日期（clock），跨天 → flush+按策略 fsync 旧文件 → 打开新文件；`keep>0` 时删除早于 keep 天的旧文件

- [ ] **Step 1: 写失败测试**：可变 clock：t0=2026-11-01 23:59:50 写入 → 前进到 11-02 00:00:10 再写 → 存在两个日期文件、各行归属正确；keep=1 → 11-03 再写时 11-01 文件被删；`{d}` 占位路径正确展开
- [ ] **Step 2: 确认失败** → **Step 3: 实现** → **Step 4: 通过** → **Step 5: Commit** `feat: daily-rotating file sink with injectable clock`

### Task 11: sink 故障隔离 + 熔断 + shutdown 超时

**Files:**
- Create: `tests/test_fault.cpp`（含 `FaultSink` 测试替身：`write` 前 N 次返回 false）
- Modify: `src/backend.cpp`（熔断状态机）

**Interfaces:**
- Consumes: Backend/Sink/Stats
- Produces: Backend 行为——单 sink 单次失败：`sink_errors++`，**其余 sink 本批照写**，循环不退出；同一 sink 连续失败 ≥ `breaker_threshold`(默认3，Backend ctor 注入) → 该 sink 禁用 `breaker_cooldown`(默认10s，注入) 后恢复重试；`shutdown(timeout)` 在 sink 持续失败时按时限返回不挂死

- [ ] **Step 1: 写失败测试**：{FaultSink(前5次失败), FileSink(ok)} 注入 Backend：写 10 条 → `sink_errors==5`（阈值3 触发熔断，FaultSink 实际收到 write 调用 ≤5）→ ok sink 文件 10 行全在；熔断注入 cooldown=50ms → 冷却后 FaultSink 恢复（后续成功）；FaultSink 永远失败 + `shutdown(200ms)` → 调用在 ≤1s 内返回
- [ ] **Step 2: 确认失败** → **Step 3: 实现**（Backend ctor 增 `breaker_threshold`/`breaker_cooldown` 两参，默认 spec 值）
- [ ] **Step 4: 通过** → **Step 5: Commit** `feat: sink fault isolation and circuit breaker`

### Task 12: 多线程压力 + 守恒 + sanitizers

**Files:**
- Create: `tests/test_stress.cpp`

**Interfaces:**
- Consumes: 全部公开 API
- Produces: 回归防线（Review Focus 1/2/5 的钉子）

- [ ] **Step 1: 写测试**（此任务测试先红后绿意义在压力，直接写全）：
  - **Block 零队列丢弃**：8 线程 × 20000 条（含 >256B 长消息各 100 条）→ `shutdown` → accepted 与各 sink persisted 分项对账
  - **DropNewest 守恒**：容量 1024、溢出 DropNewest、后端故意慢（faultsink 风格 sleep sink）→ 分别核对 accepted、queue_dropped、shutdown_abandoned 与 sink persisted；queue_dropped > 0 时 stderr 出现汇总行
  - **超长消息完整**：拼 300B 已知串 FLUX_INFO → shutdown → 文件中找到完整串
- [ ] **Step 2: 跑通**（Release 构建，<60s）
- [ ] **Step 3: sanitizers** — `cmake -B build-tsan -DFLUX_SANITIZE=tsan && cmake --build build-tsan && ctest --test-dir build-tsan`，期望 0 报告；同法 asan
- [ ] **Step 4: Commit** — `test: stress conservation and sanitizer gates`

### Task 13: 基准 + 示例 + README

**Files:**
- Create: `bench/CMakeLists.txt`, `bench/bench_producer.cpp`（单/多线程 push ns/op）、`bench/bench_e2e.cpp`（端到端 msg/s）、`bench/bench_latency.cpp`（p50/p99/p999 生产→可见，后台采样线程）、`examples/basic.cpp`
- Modify: `README.md`（构建、三行用法、性能数字占位）、`CMakeLists.txt`（接 bench）

**Interfaces:**
- Consumes: 公开 API
- Produces: `-DFLUX_BUILD_BENCH=ON` 时的 `flux_bench_*` 目标与 `flux_example`

- [ ] **Step 1: 写 bench 与 example**（example：init → 三种级别 → shutdown，作为最小用法文档）
- [ ] **Step 2: Release 构建跑通** — `./build/bench/flux_bench_producer` 输出 ns/op；记录数字到 README（对照 spec §7 目标：生产端 ≤300ns、聚合 ≥8M msg/s、后端 ≥8M msg/s；未达标记录差距，不阻塞交付——优化属 M3）
- [ ] **Step 3: 全量回归** — `ctest --test-dir build --output-on-failure` 全绿
- [ ] **Step 4: Commit** — `feat: benchmarks, example and README`

---

## 完成定义

- 全部 checkbox 完成、`ctest` 全绿（含 tsan/asan 两套）
- spec §7 基准数字记录在 README（达标或标注差距）
- 队列实现统一位于 `src/detail/mpmc_queue.hpp`，仓库根目录不保留旧副本
