# flux-log 性能优化计划

目标：在保持异步日志语义、生命周期和统计口径不变的前提下，逐阶段降低生产端开销、后端串行开销和 sink I/O 开销。

每个阶段独立完成：实现、格式化、GCC 13 构建、功能 smoke test、对应压测、记录结果并单独提交。没有通过当前阶段验证前，不进入下一阶段。

## 基线

- 单线程生产：约 221,804 msg/s。
- 4 线程生产：约 566,119 msg/s。
- 文件端到端：约 737,591 msg/s，但默认 `DropNewest` 下 1,000,000 条仅 accepted 254,909 条。

## 阶段 1：生产端格式化

- 移除每条日志的 `std::vector<std::string>`、`std::ostringstream` 和逐参数临时字符串。
- 使用 GCC 13 的 `std::format_to` 写入可复用的 thread-local staging 字符串。
- 保持 `noexcept`：格式化异常计入 `format_failed`。
- 验收：参数值、缺少占位符、超长消息和异常路径行为不变；单线程生产吞吐和分配次数记录。
- 当前结果：单线程 225,905 msg/s（基线 221,804）；4 线程 549,159 msg/s（基线 566,119，存在运行噪声和后端竞争）。示例输出与 6 位微秒时间戳验证通过。

## 阶段 2：生产端元数据缓存

- Logger 创建时缓存 `name_id`。
- 每线程缓存最近的 source file basename 到 `file_id` 的映射。
- `tid` 改为 thread-local 缓存，移除每条日志的 `gettid()` 调用。
- 验收：多 logger、跨线程 source file、re-init 后符号仍正确。
- 当前结果：name/file ID 与 tid 已缓存，GCC 13 构建和多线程示例通过；压测受共享统计锁和后端串行处理影响较大，300,000 条单线程约 209k～211k msg/s，4 线程约 231k～233k msg/s，未观察到稳定收益，后续阶段需先消除统计锁和逐条后端处理。

## 阶段 3：统计路径去锁

- `accepted`、`queue_dropped`、`format_failed` 等热路径计数改为原子或线程本地计数。
- 后端统计按 sink 使用固定 sink id，避免每条日志锁 `unordered_map`。
- 验收：多线程统计守恒和 `stats()` 结果一致。

## 阶段 4：后端批量处理

- 一次批量出队 128～512 条消息。
- 一批内完成格式化，并按 sink 收集 `string_view`。
- 每个 sink 每批调用一次 `write(span)`。
- 验收：日志顺序、sink 过滤、error/panic flush 和 shutdown 排空语义不变。

## 阶段 5：Formatter 预解析和固定缓冲

- `init()` 时将 pattern 解析为 token 数组。
- 后端使用固定/可复用缓冲，移除每条日志的 `find/replace`、`ostringstream` 和多次字符串构造。
- 缓存秒级日期前缀，只对微秒部分做整数格式化。
- 验收：自定义 pattern、默认紧凑格式、6 位微秒和 basename 输出一致。

## 阶段 6：FileSink I/O

- 用文件描述符和 `write/writev` 替换 `ofstream <<`。
- 内存维护当前文件字节数，轮转前不再调用 `filesystem::file_size()`。
- 轮转操作只在达到边界时执行。
- 验收：短写、失败、轮转、flush 和文件内容守恒。

## 阶段 7：队列唤醒和并发细节

- 增加 sleeping 状态，只在后端确认睡眠时通知。
- 检查 MPMC sequence、缓存行布局和 `LogItem` 移动成本。
- 评估 `pin_backend` 和批量大小。
- 验收：MPMC 守恒、无死锁、TSAN/ASAN（工具链可用时）。

## 阶段 8：基准重构

- 拆分 filter-only、format-only、enqueue、backend 和 e2e benchmark。
- 记录 accepted、queue_dropped、shutdown_abandoned、sink_failed、persisted。
- 将结果写入 README，避免把丢弃吞吐误认为落盘吞吐。
