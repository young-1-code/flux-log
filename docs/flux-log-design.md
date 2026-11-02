# flux-log 异步日志组件总体设计

- 状态：草案 v1.1（已过实现者评审，见 §12）
- 日期：2026-11-01
- 标准：C++20（gcc ≥ 13 / clang ≥ 16，依赖 `std::format`、`std::source_location`、`jthread`）
- 评审约定：文中标注 **[我的选择]** 的条目为实现默认值，可逐条否决；§12 为必须由你确认的问题

---

## 1. 概述

flux-log 是面向单进程多线程程序（典型场景：交易系统的订单/行情/风控路径）的高性能异步日志组件。核心思路：

> **生产线程只做"级别过滤 + 取时间戳 + 参数格式化 + 入队"四件事，其余一切（行组装、落盘、轮转、刷盘）都交给唯一的后端线程。**

### 1.1 目标

| 目标 | 量化指标（设计目标，以基准验证） |
|---|---|
| 生产端低开销 | 热路径单条 ≤ 300 ns（≈3.3M msg/s/线程，64B 消息、无堆分配） |
| 高吞吐 | 4 线程并发生产聚合 ≥ 8M msg/s；后端落盘 ≥ 8M msg/s |
| 永不拖垮业务 | 默认溢出策略为"丢弃+计数"，日志调用 `noexcept`，绝不抛出、默认不阻塞 |
| 可控丢失窗口 | 崩溃时未刷盘窗口 ≤ flush_interval（默认 200 ms）+ 队列排空时间 |
| 可靠关闭 | 正常 `shutdown()` 排空队列、flush、join；超时则放弃剩余队列并计数 |

### 1.2 非目标（v1 不做）

- 结构化/JSON 输出（预留 LogItem 扩展位，见 §3.2）
- 网络 sink（TCP/UDP/gRPC）、syslog
- 配置文件解析（仅程序化 API）
- 多进程汇聚、日志采集
- 多 sink 组路由 / 按 logger 分文件（v2 方向；Q9 已确认 v1 单组）
- 运行期增删 sink（sink 集合在 `init()` 时固定）
- `fork()` 场景支持

### 1.3 反目标（做了就算失败）

- 日志调用抛异常或导致业务线程崩溃
- 热路径出现锁、堆分配争用（除超长消息溢出的罕见路径）
- 未调用 `shutdown()` 时静默丢队列余量且无告警

---

## 2. 总体架构

```
        业务线程 ×N（生产者）                        后端线程 ×1（消费者）
┌──────────────────────────────┐      ┌────────────────────────────────────┐
│  FLUX_INFO(log, "oid={}", x) │      │  drain 循环（批量出队）              │
│    1) should_log 原子级别过滤  │      │        │                           │
│    2) clock_gettime 取 ns     │      │        ▼                           │
│    3) std::format_to_n 参数   │      │  PatternFormatter 行组装            │
│       格式化到 thread_local    │      │  （时间/级别/tid/文件行号 + 文本）    │
│       staging LogItem         │      │        │                           │
│    4) mpmc 队列 try_push      │      │        ▼                           │
│    5) pending++ → 按需 notify  │      │  SinkGroup（File / Rotating /      │
│        溢出→丢弃+计数          │      │  Daily / Stdout）批量写 + 刷盘/轮转 │
└──────────────┬───────────────┘      └────────────────────────────────────┘
               │   LogItem（定长 ≈288B，内联文本）│        ▲
               ▼                                     │ flush 信号 / 周期到期
        ┌────────────────────────────┐    ┌─────────┴──────────┐
        │ Vyukov 有界 MPMC 环形队列    │    │ condvar + 状态标志   │
        │ （复用 src/detail 中的      │    │ （空闲批处理唤醒）    │
        │  Vyukov MPMC 队列）         │    │                       │
        │  现代化改造，算法不变）       │    └────────────────────┘
        └────────────────────────────┘
```

一次日志调用的时序：

```
业务线程                          后端线程
   │ FLUX_INFO(...)
   ├─ 级别过滤(失败即返回,~1ns)
   ├─ 取时间戳(~20ns)
   ├─ 参数格式化→thread_local缓冲
   ├─ try_push(LogItem)
   │    ├─ 成功: pending.fetch_add; 若后端睡眠→notify
   │    └─ 满:   按溢出策略(默认丢弃+queue_dropped++)
   ▼
                                    └─ 被 notify/超时唤醒
                                       ├─ while(try_pop) 批量出队
                                       ├─ 逐条行组装→per-sink 行缓冲
                                       ├─ 批量 write() 到各 sink
                                       ├─ 到达 flush 线索→flush()
                                       └─ 队列空→置睡眠标志→等待
```

---

## 3. 关键设计决策

| # | 决策点 | **[我的选择]** | 主要备选 | 否决理由 |
|---|---|---|---|---|
| D1 | 队列 | 复用 Vyukov MPMC 有界队列（定长元素改造） | disruptor 每线程 staging；阻塞队列+mutex | disruptor 复杂度高，留作 M3；阻塞队列热路径有锁 |
| D2 | 队列负载 | 定长 `LogItem`，文本内联（超长走堆溢出） | `std::string` 每条堆分配 | 堆分配在热路径，且多线程 malloc 争用 |
| D3 | 溢出策略 | 默认 `DropNewest`+计数+告警；可配 `Block` | 覆盖最旧 | Vyukov FIFO 结构无法 O(1) 丢最旧 |
| D4 | 格式化时机 | 生产者只格式化**消息参数文本**；整行组装在后端 | 全部后端格式化（NanoLog 式） | NanoLog 需格式串驻留+类型擦除，v1 复杂度过高，留作 M3 |
| D5 | 时间戳 | 生产者 `std::chrono::system_clock`（realtime ns），`thread_local` 缓存 | TSC（rdtsc） | TSC 需校准与漂移处理，留作 M3 选项 |
| D6 | 唤醒机制 | condvar + 睡眠状态标志（仅在睡眠→唤醒转换时 syscall） | 纯自旋；`atomic::wait`/futex | 自旋烧 CPU；atomic wait 收益小，M3 再评估 |
| D7 | 线程模型 | 全局单后端 `std::jthread`，多 logger 共享 | 每 logger 一线程 | 线程数浪费，顺序性难保证 |
| D8 | 生命周期 | 显式 `flux::init(cfg)` / `flux::shutdown()` | 全静态魔法初始化 | 规避静态析构顺序问题 |
| D9 | 落盘丢失窗口 | 周期 flush（默认 200 ms）+ ERROR 及以上立即 flush | 每条 fsync | 每条 fsync 吞吐崩塌（数量级损失） |
| D10 | 并发模型 | 线程 + 队列；**不使用协程** | 协程承载后端 I/O | 唯一后端执行流是纯顺序循环，无挂起点；需等待的两处（空队列、Block/排空）用 condvar/futex 即最优；生产端绝不能 await；spdlog/NanoLog/folly 同样不用 |
| D11 | 磁盘 I/O | 缓冲 `write()`（页缓存）+ 独立 fsync 策略 | io_uring | 批量缓冲已把 syscall 摊到 ~10²–10³ 次/s（<0.5% CPU），无在途 I/O 可重叠；fsync 延迟设备主导，io_uring 不能消除只能重叠，而重叠用"独立 fsync 线程"即可获得；io_uring 带 Linux 专有双路径与环境禁用风险。基准证明写路径成瓶颈时（M3）再评估 |

### 3.1 队列（D1）

沿用 Vyukov 有界 MPMC 环形队列算法（`src/detail/mpmc_queue.hpp`），并做两点包装改造：

- **定长元素**：cell 直接承载 `LogItem`（目标 288B）；含 spill 所有权，必须使用移动语义；
- **等待/通知包装层** `BackendQueue`：内部聚合 `pending` 原子计数与溢出策略，接口为 `try_push(LogItem&)` / `pop_batch()` / `wait_and_pop(timeout)`。

容量以"条数"配置，必须 2 的幂，默认 16384 条（按 288B 元素约 4.5 MiB）。

### 3.2 队列负载：LogItem（D2）

```cpp
enum class Level : std::uint8_t { Trace, Debug, Info, Warn, Error, Panic, Off };

struct LogItem {                       // 定长；含 spill 所有权，必须移动而非字节复制
    std::uint64_t ns;                  // 生产者采集的 realtime 纳秒
    std::uint32_t tid;                 // thread_local 缓存的 gettid()
    std::uint16_t text_len;            // 有效长度，最大 65535；超限截断并计数
    Level         level;
    std::uint16_t name_id;             // logger 名（注册表驻留表索引，见 §3.6）
    std::uint16_t file_id;             // 文件 basename 驻留表索引
    std::uint32_t line;                // source_location 行号
    char          text[256];           // 内联文本
    std::unique_ptr<char[]> spill;     // 超长消息（> inline_cap）的堆后备，罕见路径
};
```

- 生产者用 `thread_local LogItem staging` + `std::format_to_n` 直接格式化进内联缓冲：**常规消息零堆分配**；
- 256B 以上消息走 `spill` 堆分配；超过 65535B 截断、计入 `oversize_truncated` 并限频告警；
- logger 名和文件 basename 经注册表**驻留**（append-only 表，`name_id/file_id` 索引），后端按 id 反查；
- `file/line` 由宏经 `std::source_location`（编译期）传入，以 `file_id + line` 独立字段保存；`function` 暂不进入 LogItem。

### 3.3 溢出策略（D3）

```cpp
enum class OverflowPolicy : std::uint8_t { DropNewest, Block };
```

- `DropNewest`（默认）：`try_push` 失败即返回，`queue_dropped` 原子计数 +1；后端每秒合并输出一条 `"... N logs dropped ..."` 到 stderr（不经队列，避免自激）；
- `Block`：自旋 + `yield` 重试直到成功（适用于审计类不可丢日志，配置在 logger/sink 级别）；
- `stats()` 暴露按生命周期拆分的计数：`accepted`、`queue_dropped`、`shutdown_abandoned`、`format_failed`、`oversize_truncated`、`sink_filtered`、`sink_failed`、`pre_init`、`post_shutdown`，并按 sink 统计 `persisted`。

### 3.4 格式化拆分（D4）

- 生产者：`std::format_to_n` 仅渲染用户参数（`"oid={} px={}"` → `"oid=42 px=100.5"`），复用 thread_local 缓冲；
- 后端：`PatternFormatter` 把 `ns/tid/level/logger 名/file:line` 与 text 组装成完整行，追加进 per-sink 行缓冲（`std::span<const std::string_view>` 批量交给 sink）；
- Pattern 语法对齐业界惯例：`{ts}{level}{tid}{name}{file}{line}{msg}`，init 时解析为 token 序组（`consteval` 编译期校验默认 pattern）；
- `{ts}` 渲染默认本地时区 `%Y-%m-%d %H:%M:%S.ffffff`（6 位微秒）；默认行格式为 `{ts} [{level}][{tid}][{name}][{file}:{line}]{msg}`，pattern 可覆盖；DailyFileSink 轮转边界取本地午夜；主机无 tzdata 时回退 UTC 并 stderr 告警一次（已确认：本地时区）。

### 3.5 后端线程与刷盘（D6/D7/D9）

- `std::jthread` + `stop_token`：退出条件 = 收到 stop 且队列排空；
- 主循环：`pop_batch`（一次最多 N 条，如 256）→ 组装 → 批量 `write()`（`writev`/合并缓冲）→ 判断 flush 线索：
  - 距上次 flush ≥ `flush_interval`；或
  - 批内出现 `level ≥ Error`；或
  - 收到显式 `flush()` 请求（同步语义：flush 序号达到即认为完成）；
- `flush_interval` 由全局后端统一控制；`fsync` 策略由每个 sink 独立配置为 `Never / OnRotation / Always`（默认 sink 策略为 `OnRotation`）；
- 可选 `pin_backend = true`：绑核 + 空闲自旋（延迟最低，烧一个核，交易进程常可接受）。

### 3.6 Logger 注册表与 sink 组（D8）

- **单组路由（Q9 已确认）**：v1 所有 logger 共享同一组 sink；`flux::get(name)` 按名取 logger，首次创建即注册；
- 注册表仅维护一张 **append-only** 的 `name_id → 名字` 驻留表，后端无锁读；sink 集合 `init()` 时固定；多组路由为 v2 方向，届时再引入 `sink_id`；
- 溢出策略是**每 logger** 的生产端行为（`Block` 只阻塞该调用线程自己，不拖累其他线程），与单组队列正交，审计类 logger 仍可单独配 `Block`；
- logger 级别阈值 `std::atomic<Level>`，运行时可调（`log.set_level()`），热路径一次 relaxed load。
- 新建 logger 默认级别为 `Info`；未知级别、溢出策略、fsync 策略和非法配置一律拒绝初始化，不静默回退。

---

## 4. 模块划分与目录结构

```
flux-log/
├── include/flux/
│   ├── log.hpp                # 公开总头文件
│   ├── level.hpp              # Level 枚举
│   ├── config.hpp             # Config + 各策略枚举（designated initializers）
│   ├── logger.hpp             # Logger（模板 log()，热路径内联）
│   ├── macros.hpp             # FLUX_INFO/DEBUG/... 宏（source_location 捕获）
│   ├── sink.hpp               # Sink 虚接口（批量写摊薄虚调用开销）
│   ├── formatter.hpp          # Formatter 抽象 + PatternFormatter
│   └── registry.hpp           # init()/get()/shutdown()/stats()
├── src/
│   ├── backend.cpp            # 后端线程主循环、BackendQueue
│   ├── log_item.hpp           # LogItem（内部）
│   ├── detail/mpmc_queue.hpp  # Vyukov 有界 MPMC 队列
│   ├── formatter.cpp          # pattern 解析与行组装
│   └── sinks/{file,rotating,daily,stdout}.cpp
├── tests/                     # GTest 单测 + 多线程压力
├── bench/                     # google-benchmark：生产端 ns/op、端到端 msg/s、延迟分布
├── examples/
└── CMakeLists.txt
```

依赖：仅标准库（`std::format`）；测试用 GTest + google-benchmark（构建可关）。

---

## 5. 公开 API 草案

```cpp
// main 入口（单组：所有 logger 共享同一组 sink）
flux::init(flux::Config{
    .queue_items    = 16384,                        // 2 的幂
    .overflow       = flux::OverflowPolicy::DropNewest,  // 队列默认溢出策略
    .flush_interval = std::chrono::milliseconds{200},
    .pin_backend    = false,
    .sinks = { flux::make_file_sink("logs/trade.log",
                                    flux::Rotate{.max_size = 512 << 20, .keep = 7}),
               flux::make_stdout_sink() },
});

auto& order = flux::get("order");                   // 按名取，首次创建即注册
auto& audit = flux::get("audit");
audit.set_overflow(flux::OverflowPolicy::Block);    // 审计类：只阻塞自己，绝不丢
order.set_level(flux::Level::Info);

FLUX_INFO (order, "fill oid={} px={}", oid, px);    // 宏捕获 source_location
FLUX_WARN (order, "reject oid={} reason={}", oid, r);
FLUX_ERROR(order, "risk breach {}", detail);        // ≥Error 自动即时 flush

flux::flush();                                      // 显式同步刷盘
flux::Stats s = flux::stats();                      // 生命周期计数 + 按 sink persisted
flux::shutdown();                                   // 排空+flush+join（main 末尾必调）
```

宏展开形式：

```cpp
#define FLUX_LOG(logger, lvl, fmt, ...)                                  \
    do {                                                                 \
        if constexpr (static_cast<int>(lvl) >= FLUX_LOG_COMPILE_LEVEL) { \
            if ((logger).should_log(lvl)) [[unlikely]] {                 \
                (logger).log(lvl,                                        \
                             std::source_location::current(),            \
                             std::format_string{fmt}                     \
                             __VA_OPT__(, ) __VA_ARGS__);                \
            }                                                            \
        }                                                                \
    } while (0)

#define FLUX_INFO(logger, fmt, ...) \
    FLUX_LOG(logger, flux::Level::Info, fmt __VA_OPT__(, ) __VA_ARGS__)
// TRACE/DEBUG/WARN/ERROR/PANIC 同构；FLUX_LOG 仅内部使用（lvl 须为编译期常量）
```

四层递进过滤：编译期裁剪（`if constexpr` + `-DFLUX_LOG_COMPILE_LEVEL`）→ 运行期级别检查（relaxed 原子读，未命中 ~1ns）→ `std::format_string` 编译期校验格式串 → 格式化入队。

宏级别裁剪：`-DFLUX_LOG_COMPILE_LEVEL=2`（Info）使 `Trace/Debug` 编译期消失（`if constexpr` 分支）。

生命周期契约：

- `flux::get()` 允许在 `init()` 之前调用；Logger 和符号表跨轮次复用，`re-init` 只替换后端与 sink；
- `init()` 要求至少一个 sink，未知枚举、非法容量、负时间等配置直接抛出配置错误；
- `shutdown()` 发布停止标志后拒绝新日志，已进入提交阶段的日志允许完成；超时即放弃剩余队列并计入 `shutdown_abandoned`；
- `shutdown()` 之后的新日志计入拒绝统计；内部告警绕过 flux 队列，直接写 stderr 并限频。

---

## 6. C++20 特性清单

| 特性 | 用途 |
|---|---|
| `std::jthread` + `stop_token` | 后端线程生命周期与协作式退出 |
| `std::source_location` | 宏无参捕获 `file/line/function` |
| `std::format` / `format_to_n` | 生产端参数渲染（编译期格式串校验） |
| concepts | `Formatter` 与内部模板约束（Sink 为**虚接口**：异构组需运行时多态，批量写摊薄 vtable 开销） |
| `std::span` | 后端→sink 的批量行传递 |
| designated initializers | `Config` 可读配置 |
| `consteval` | 默认 pattern 编译期校验 |
| `std::chrono` 日历 | DailyFileSink 按天轮转边界计算 |
| `[[likely]]/[[unlikely]]` | 热路径分支（级别过滤、内联命中） |
| `std::atomic::wait/notify` | （M3 备选）替代 condvar |

---

## 7. 性能目标与基准方案

| 基准 | 目标 |
|---|---|
| 生产端单线程稳态（64B 消息） | ≤ 300 ns/msg |
| 生产端 4 线程聚合 | ≥ 8M msg/s |
| 后端落盘（含行组装、顺序写、批 write） | ≥ 8M msg/s |
| 空闲→日志可见延迟 | ≤ 10 µs（condvar）；`pin_backend` ≤ 1 µs |
| 关闭排空（16384 条满队列） | ≤ 50 ms |

bench 内容：`bench/` 下 (a) 生产端 ns/op 直测；(b) 多线程端到端 msg/s；(c) p50/p99/p999 生产→可见延迟（后台采样线程）；(d) 后端独占吞吐。另以 TSAN/ASAN 跑压力用例。

---

## 8. 可靠性与错误处理

- 所有公开日志入口 `noexcept`；内部捕获一切异常；
- sink 写失败（磁盘满等）：计数 + 首次错误到 stderr 一次（限频），后端不退出；连续失败可配"熔断 N 秒"；
- 队列丢弃、shutdown 放弃、格式化失败和超长截断分别计数；每秒最多一条 stderr 汇总；
- `shutdown()` 语义：拒绝新日志（宏内级别检查失败路径）→ 排空队列 → flush 全 sink → join，带超时参数；
- 未调 `shutdown()` 的兜底：registry 静态析构 best-effort 排空并打 stderr 警告（反目标兜底）；
- 崩溃丢失窗口 = flush_interval（默认 200 ms）——交易关键路径如需收紧，把该 logger 的 sink 配 `flush_interval` 更低或对 Error 即刷（默认已即刷）。

---

## 9. 测试计划

1. 单元：队列并发正确性（MPSC/SPMC/MPMC 压力 + TSAN）、pattern 解析、轮转边界（跨天/跨大小）、溢出策略、级别过滤与编译期裁剪；
2. 集成：8 线程 × 1M 条 `Block` 策略零队列丢弃校验；`DropNewest` 校验 accepted、queue_dropped、shutdown_abandoned 与各 sink persisted 的分项对账；
3. 异常注入：sink 抛异常/写失败（ENOSPC 模拟）不影响业务线程；
4. 基准：§7 全项入 CI（可选 nightly）。

---

## 10. 里程碑

| 阶段 | 内容 | 出口标准 |
|---|---|---|
| M1 最小闭环 | Config/init/shutdown、Logger+宏、BackendQueue（溢出+pending）、后端线程、PatternFormatter、FileSink、StdoutSink | 多线程压力零丢失（Block）；端到端冒烟 |
| M2 生产化 | Rotating/Daily sink、fsync 策略、stats、错误熔断、benchmark 全套、TSAN 通过 | §7 基准达标 |
| M3 性能深化（按需，逐项以基准驱动） | 延迟格式化（格式串驻留）、atomic wait/futex、TSC 时钟、绑核自旋、每线程 staging | 每项独立评估，不达预期即回退 |

---

## 11. `src/detail/mpmc_queue.hpp` 的实现约束

实现基于 Vyukov 算法，必须满足：

1. **默认构造不初始化 `_max_size`**：当前 `size()/capacity()` UB；
2. **容量校验晚于 `new[]`**：抛异常即泄漏；改为先校验后分配；
3. `init()` 与构造函数重复：删除 `init()`，只留构造；
4. 裸 `new[]/delete[]` → `std::unique_ptr<cell_t[]>`；
5. `size()` 的 `_max_size` 截断逻辑修正（无符号回绕下 `enqueue-dequeue` 天然正确，直接返回即可）；
6. 命名空间 `tiny` → `flux::detail`，`cacheline_pad_t` 对齐到 `std::hardware_destructive_interference_size`（不足 64 回退）。

---

## 12. 待确认问题（影响设计的才列）

> 2026-11-02 确认记录：Q1–Q9 及生命周期、shutdown 超时、统计口径、flush/fsync 层级、消息上限、错误处理、默认级别、非法配置和告警通道均已确认。

| # | 问题 | 影响 | 缺省（你不答我就按此实现） |
|---|---|---|---|
| Q1 | 溢出默认策略：热路径"丢弃+计数"（我的选择）还是绝不丢（Block）？ | D3、热路径延迟 | DropNewest |
| Q2 | 崩溃丢失窗口 200 ms 是否可接受？关键 logger 是否需要更短/每条同步？ | D9、磁盘吞吐 | 200 ms + Error 即刷 |
| Q3 | 输出目标：本地文件+按大小/按天轮转即可？stdout 要吗？未来要网络 sink 吗？ | sink 集合与 M2 范围 | 文件轮转+stdout |
| Q4 | 依赖约束：仅标准库？测试/bench 可否引入 GTest、google-benchmark？ | 构建与 CI | 标准库 + 可关的测试依赖 |
| Q5 | 工具链实际版本（gcc/clang、libstdc++）？`std::format` 需 gcc≥13 | 编译门槛 | gcc 13 |
| Q6 | 是否需要结构化（JSON）输出或显式字段（file/line 作为独立列）？ | LogItem 布局 | 纯文本行 |
| Q7 | 交付形态：静态库 + CMake，还是 header-only？ | 目录结构与集成 | 静态库 |
| Q8 | ~~行时间戳/Daily 轮转的格式与时区约定？~~ **已确认（2026-11-02）：本地时区 + `%Y-%m-%d %H:%M:%S.ffffff`（6 位微秒）** | D5 渲染、DailyFileSink | ✅ 已定 |
| Q9 | ~~多 sink 组路由 v1 必须吗？~~ **已确认（2026-11-01）：v1 单组，多组留作 v2** | §5 API、注册表 | ✅ 已定 |
