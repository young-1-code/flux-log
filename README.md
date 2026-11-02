# flux-log

## 构建和运行

```bash
cmake -S . -B build-gcc13 \
  -DCMAKE_CXX_COMPILER=/home/young/miniforge3/envs/flux-gcc13/bin/aarch64-conda-linux-gnu-g++ \
  -DFLUX_BUILD_BENCH=ON -DFLUX_BUILD_EXAMPLES=ON
cmake --build build-gcc13 -j
./build-gcc13/examples/flux_example
```

## 压测入口

```bash
./build-gcc13/bench/flux_bench_filter 10000000
./build-gcc13/bench/flux_bench_format 1000000
./build-gcc13/bench/flux_bench_producer 4 1000000
./build-gcc13/bench/flux_bench_e2e 1000000 /tmp/flux.log
```

阶段性结果（GCC 13.4，AArch64）:

- 生产端：单线程约 630k msg/s，4 线程约 1.09M msg/s（阶段 5）；
- 文件端到端：约 776k msg/s（阶段 6）；
- 默认 `DropNewest` 下，端到端结果必须同时查看 `accepted`、`queue_dropped` 和落盘行数。
