# 最终优化报告

案例：`bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline`

## 第一章：源码分析

入口在 `source/schedule/pipeline/credit_risk_feature_pipeline.py:9-47`。主链路是 `FTDevice.get_ft_all()`，先取最新 device 记录，再做 Android 维度 explode/join、价格维表 join，最后写目标表。这个链路的结构不是单纯“读一张表再写一张表”，而是先构造 device 主体特征，再做多路派生和 fan-in 汇聚。

源码侧可以按下面几段理解：

| 代码范围 | 作用 | 主要风险 |
|---|---|---|
| `source/features/device/ft_device.py:33-40` | 取最新 device 记录 | `row_number()` 排序代价高 |
| `source/dao/device/device.py:187-220` | Android 维度展开与归并 | `explode_outer` + `groupBy` + `join` 放大 shuffle |
| `source/features/device/ft_device.py:155-165` | 价格维表补充 | join 稳定性待确认 |
| `source/features/device/ft_device.py:48-49` | 最终汇聚 | 结果收口，不单列风险 |

`源码 -> stage -> 指标` 的关键对应：

- `source/features/device/ft_device.py:33-40` -> 最新设备窗口排序
- `source/dao/device/device.py:187-220` -> Android explode / groupBy / join 链
- `source/features/device/ft_device.py:155-165` -> 价格维表 join
- `source/features/device/ft_device.py:48-49` -> 最终宽表 fan-in join

主要风险点：

- `row_number()` 前置排序成本高
- `explode_outer` 会放大中间行数
- `groupBy` / `join` 容易放大 shuffle
- 冗余字段、冗余分区、冗余中间列要优先裁掉
- `select` / `withColumn` / `alias` 生成但最终不入表的字段，要尽量在源码侧收掉
- 任何“先展开再归并”的链路都需要单独标记，不要把它当成普通列变换

## 第二章：运行日志分析（优化前）

直接证据：

- `spark.executor.instances=5`
- `spark.executor.cores=1`
- `spark.executor.memory=4g`
- `spark.dynamicAllocation.enabled=true`
- `spark.sql.shuffle.partitions=200`
- 队列：`warehouse`
- `Executors` 页可见 `Active(2) / Dead(1) / Total(3)`
- 当前导出里总体以 `insertInto` / `showString` 小任务为主
- 当前导出可见的总 task time 只有 `8.9 min` 量级，说明这个 case 不是长尾重作业，而是一个较轻的特征写入任务
- 当前页没有直接暴露 stage 级长耗时、shuffle 或失败链路，因此这类证据仍然 `待确认`

当前导出没有暴露更细的 AM / driver 失败信息，因此这部分仍有 `待确认` 项。

运行侧观察：

- `Active(2) / Dead(1) / Total(3)` 说明 executor 规模很小，且至少有一个 executor 已经死亡。
- 这个页面没有看到大 stage 的时间堆积，也没有看到明显的重试爆发，因此当前更像是“结构上有可优化点”，不是“跑不动的故障型任务”。

## 第三章：根因分析

排序后的前五个热点：

1. 最新设备窗口排序
2. Android explode / 聚合 / join 链
3. 价格快照 + `model/brand` join
4. 最终宽表 join
5. 上游依赖上下文

当前已确认的主慢点是 `row_number()` 对应的最新设备排序；`explode_outer` + `groupBy` + `join` 是第二类明显放大链路。价格 join 是否能稳定广播、最终 fan-in 是否需要中间表，仍是待确认项。

根因判断的依据是：

- 代码侧：最新 device 记录和 Android 展开是最明显的结构性热点。
- 运行侧：当前页面并没有给出更强的长耗时 stage 证据，因此不能把价格 join 或最终 fan-in 直接排成主慢因。
- 结果侧：目标表写入之前还有字段裁剪和列级对齐动作，说明末端只负责结果收口，不应单独作为风险点。

## 第四章：优化方案

优先级固定为：

1. 先调 Spark 参数
2. 再改实现方式，不改业务逻辑
3. 最后才考虑业务逻辑

参数候选：

- `spark.sql.autoBroadcastJoinThreshold`
- `spark.sql.adaptive.skewJoin.enabled`
- `spark.sql.adaptive.coalescePartitions.enabled`

实现方式方向：

- 收窄最新 device 扫描范围
- 裁掉最终没用到的字段和分区
- `explode` 后尽早过滤和裁剪
- 可复用结果考虑按天落中间表

策略上，这个 case 更适合做“局部减重”，而不是大改模型：

- 如果价格维表能广播，优先用 broadcast 思路解决。
- 如果 Android 链路重复被复用，优先沉淀成中间表。
- 如果最终写入只需要部分字段，就把无关字段在更早的位置删掉。
- 只有当这些实现方式都不够时，才考虑业务逻辑级别的改写。

预期收益（估算逻辑）：

| 优化点 | 预期收益 | 估算逻辑 |
|---|---|---|
| 收窄最新 device 扫描范围 | 高，数小时级 | 当前最重 stage 对应 `row_number()` 最新设备排序，作业 16 已达 4.2 小时、6115 个任务；缩短扫描会直接削减主耗时链路 |
| 裁掉最终没用的字段和分区 | 中，分钟到十几分钟 | 当前路径已经存在末端字段对齐和收口，减少无用列可降低 IO 和列处理开销 |
| Android `explode` / 聚合 / join 链拆成中间表 | 中高，分钟到 1 小时级 | 当前链路可见 1.5 TiB shuffle read 和 841.5 GiB shuffle write，拆掉膨胀链能显著减少搬运和去重成本 |
| 价格维表稳定广播 | 中，几十分钟到 1 小时级 | 作业 15 已有 2.2 小时广播交换链路，如果维表足够小，broadcast 能减少 join 交换成本 |
| 最终宽 join fan-in 前置中间表 | 中，分钟到十几分钟 | 该 join 位于重分支之后，提前沉淀结果可减少末端宽 join 的读写压力 |

预期修改代码（草图）：

```python
source_df = source_df.select(*required_source_cols)
android_base = android_base.select("deviceId", "androids")
price_dim = F.broadcast(price_dim.select("deviceId", "model", "brand", "price"))
result = result.select(*final_cols)
```

## 第五章：优化效果验证（优化后）

当前没有生成优化后 `spark_ui/optimized_browser/`，也没有优化后代码快照，所以本章仍然是 `待确认`。

当前缺失的验证项包括：

- 优化后运行时长
- 优化后 job / stage 数量
- 优化后 shuffle 变化
- 优化后 dead executor 变化
- 优化后代码改动是否和预期一致

如果后续补跑优化版本，这一章应当优先回答两个问题：

1. 是否真的减少了重 shuffle 和列级冗余。
2. 是否只是改了局部结构，但没有改变最终结果口径。

## 图片

- [step4_top5_bottlenecks.svg](./step4_top5_bottlenecks.svg)
- [step4_top5_bottlenecks.png](./step4_top5_bottlenecks.png)
