# 优化前后对比

案例：`underiwrinting_app`

对比对象：
- 优化前：`application_1772593899018_1090326`
- 优化后：`application_1772593899018_1093299`

## 1. 总体结论

这次优化的效果是明确的：主链路从 **12.3 小时级** 收敛到 **35 分钟级**，运行时长下降到原来的约 **5%**。同时，大规模扫描、长时 shuffle stage、失败 task 和 stage 重试都明显减少。

优化前的任务仍然保留了大规模历史扫描和大 shuffle 链路，`stage 1` / `stage 3` 都是 TiB 级输入，且出现了失败 stage 和大量失败 task。优化后，主链路已经收敛到秒级到分钟级 stage，未再出现 TiB 级扫描和长时大 shuffle stage。

## 2. 关键指标对比

| 指标 | 优化前 | 优化后 | 变化 |
|---|---:|---:|---:|
| 总体运行时长 | 12.3 h | 35 min | 大幅下降，约减少 95% |
| Completed Jobs | 9 | 9 | 数量相同，但单个 job 耗时明显下降 |
| Completed Stages | 10 | 9 | 结构收敛，失败 stage 消失 |
| Failed Stages | 1 | 0 | 失败 stage 被消除 |
| 最大扫描输入 | 7.1 TiB | 未再出现 TiB 级扫描 | 大幅下降 |
| 扫描路径数 | 360 paths | 356 paths | 360 是逻辑窗口，356 是 Spark 本次实际枚举到的物理 leaf paths |
| 主扫描 stage 任务数 | 70823 | 565 | 大幅下降，约减少 99% |
| 最大 Shuffle Write | 146.2 GiB | 135.6 GiB | 降低，但仍存在中等规模 shuffle |
| 最大 Shuffle Read | 110.4 GiB | 126.0 GiB | 仍有中等规模读取，但不再是 TiB 级链路 |
| 失败 task | 928 failed / 68 failed / 3 failed 等 | 仅出现少量 killed/another attempt succeeded | 明显减少 |
| 长时 stage | 5.0 h、4.3 h、2.7 h、1.1 h、37 min | 23 min、12 min、10 min、9.7 min、5.4 min、4.6 min | 长时 stage 被压缩到分钟级 |

## 3. 优化前的主要瓶颈

### 3.1 大扫描

优化前的核心问题是 `stage 1` 和 `stage 3` 读了 7.1 TiB 级数据，且任务数高达 70,823 个。这个量级已经不是“慢 SQL”，而是“重链路批处理”。

### 3.2 长尾与失败

优化前存在：
- `Failed Stages: 1`
- `stage 7` 运行 `37 min`，并报 `FetchFailedException`
- `928 failed`、`68 failed` 这类大量 task 失败记录
- `killed: another attempt succeeded` 说明 stage 有明显重试和回收

### 3.3 冗余计算与放大

优化前源码侧的主要风险包括：
- 360 天历史扫描
- `explode` 带来的数据膨胀
- `dropDuplicates` + `join` 放大中间数据
- 双路聚合重复 shuffle
- `rank` 等中间列冗余透传

## 4. 优化后的变化

优化后最显著的变化是，原本 TiB 级扫描和小时级 stage 被收敛成了秒级到分钟级 stage：
- `stage 0` 仍然要做 `356 paths` 的 leaf files listing，但只用了 `7 s`
- `stage 1` 只有 `24 s`
- `stage 2` 只有 `24 s`，任务级别已经是 `68/68`
- `stage 3` 只有 `3 s`
- 主要 job 运行时间集中在 `4.6 min`、`5.4 min`、`5.1 min`、`23 min` 这几个阶段

这说明优化方向已经从“大规模重复扫描 + 大 shuffle”转向了“较小范围的分段执行”。

## 5. 资源消耗变化

优化前的资源消耗特点：
- 大量 task 失败和重试
- TiB 级输入
- 百 GiB 级 shuffle 写入
- 有失败 stage
- 长尾明显

优化后的资源消耗特点：
- 没有再出现 TiB 级输入 stage
- 没有失败 stage
- task 级执行时间从小时级降到秒级 / 分钟级
- 仍有一定 shuffle，但规模已经下到百 GiB 级以内的可控区间

## 6. 结果判断

这次优化是有效的，主要收益体现在：

1. **总时长显著下降**
   - 从 12.3 小时降到 35 分钟。

2. **失败链路被消除**
   - 优化前有 failed stage，优化后没有失败 stage。

3. **大扫描被压缩**
   - 优化前存在 7.1 TiB 级输入，优化后不再出现该量级。

4. **长时 stage 被替换成短 stage**
   - 优化前多个小时级 stage，优化后主要是秒级到分钟级 stage。

5. **主扫描任务数明显下降**
   - 优化前主扫描 stage 达到 70,823 个任务。
   - 优化后主扫描 stage 约 565 个任务，任务调度和执行压力显著减轻。

## 6.1 关于 360 与 356 的差异

报告里的 `360` 指的是源码里配置的 360 天逻辑窗口，`356 paths` 指的是 Spark 在这次运行里实际枚举到的物理 leaf 文件路径数。  
这两者不是一回事，也**不能直接推导为少了 4 个分区**。如果你已经确认上游分区都有文件，那么这里更合理的解释是：

- Spark 本次文件枚举的路径数就是 356；
- 这可能来自目录过滤、路径合并、某些路径未进入这次实际扫描计划；
- 也可能是源表 / 中间表 / 物化路径的枚举口径与逻辑窗口并不完全一致。

因此，这里应该只写成：**逻辑窗口是 360 天，但 Spark 本次实际枚举到 356 个物理 leaf paths**，不要再写成“缺少 4 个分区”。

## 7. 仍需关注的问题

虽然整体改善明显，但优化后仍有中等规模 shuffle：
- `stage 9` / `stage 7` / `stage 12` 依然有百 GiB 级输入和 shuffle 写入
- 说明还有进一步收敛空间，后续可以继续检查：
  - 是否还能进一步裁字段
  - 是否还能减少 `join` / `groupBy`
  - 是否还能把可复用链路再沉淀成中间表

## 8. 对应文件

- 优化前：`input/tmp_1090326/spark_ui/browser/`
- 优化后：`input/tmp_1093299/spark_ui/browser/`
- 当前对比报告：`output/underiwrinting_app/optimization_compare.md`
