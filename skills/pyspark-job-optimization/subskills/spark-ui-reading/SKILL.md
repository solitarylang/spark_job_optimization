---
name: pyspark-spark-ui-reading
description: 独立执行 Spark UI / YARN / driver / executor 日志分析，输出第 2 章素材。适用于只需要运行证据采集、环境信息和失败链路分析的场景。
---

# Spark UI / 日志分析子 skill

## 目的

只做运行证据采集和整理，不进入根因排序。目标是把 Spark UI、YARN application、AM、driver、executor、eventlog 的直接证据收拢起来。

## 输入

- `input/<case_name>/spark_ui/browser/`
- `input/<case_name>/spark_ui/` 导出的页面文本或 HTML
- `input/<case_name>/eventlog/`
- YARN application 链接或 Spark 运行日志链接

## 工作方式

1. 如果输入是 YARN application 链接，先采 ApplicationMaster，再进入 Spark 运行日志。
2. 按顺序看 `jobs`、`stages`、`executors`、`environment`、`sql`。
3. 额外补充 `eventlog`、AM、driver、executor、YARN diagnostics 中能直接看到的失败或重试证据。
4. 记录每个关键 stage 的耗时、任务数、shuffle、失败、spill、preemption 和 loss reason。
5. 统计超大 stage 数量；如果读取超过 10 亿行 / 2T 或运行超过 30 分钟的 stage 超过 4 个，要标出这一事实，后续优先考虑部分 stage 落表 / 中间表化。
5. 对于运行时间超过 30 分钟的 job 或 stage，必须继续点击对应 stage，采集 task 级别的更细运行信息，包括 task duration 分布、失败 task、shuffle、spill、长尾和是否存在 skew。
6. 如果某个 stage 的读取行数超过 10 亿，或者输入规模超过 2T，要先做量级合理性检查；重点确认上游表规模、分区裁剪、字段裁剪、是否存在重复扫描，以及是否应该提前落中间表。

## 需要重点采集的运行信息

- Application Overview：application id、name、user、queue、状态、开始结束时间、diagnostics
- Environment：Spark 版本、Spark 配置、执行参数、dynamic allocation、shuffle partitions、broadcast threshold、AQE、speculation
- Jobs：job 数量、长耗时 job、失败 / 重试 job、重复提交痕迹
- Stages：stage 级耗时、task 数、input / output、shuffle read / write、失败 reason、retry / skipped / spill
- 对于超过 30 分钟的 job / stage，必须下钻到对应 stage 的 task 详情，补充 task duration 分布、失败 task、最慢 task、shuffle、spill、长尾、skew 和是否存在明显不均衡
- Executors：active / dead / total、负载是否均衡、GC / spill / shuffle / task time、loss reason
- SQL：query plan、join / exchange / window / broadcast / stage 对应关系
- EventLog：原始 stage / task / job 证据
- AM / driver / executor / YARN diagnostics：`OOM`、`Killed by YARN`、`preempted`、`node lost`、`fetch failed`、`file not found`、`disk error`、`exit code`

## 标签判定口径

这部分是 Spark 日志分析里的同级判定逻辑，用来把采集到的运行证据映射成可复用标签。只写能从 UI / 日志直接确认的规则，不把推断写成已确认结论。

### 1. 失败与重试类

- `task_failed_label`：`final_state = failed`
- `spark_stage_failed_label`：Spark 类任务有失败 / attempt > 0
- `spark_stage_failed_gt3_label`：Spark 类任务失败 / attempt > 3
- `spark_task_retry_label`：Spark 类任务存在 Airflow / task 重试
- `spark_task_retry_gt3_label`：Spark 类任务重试次数 > 3

### 2. 时长与排队类

- `runtime_gt_1h_label`：运行时长 > 60 分钟
- `submit_wait_gt_10min_label`：排队 / submit wait > 10 分钟
- `mu_top100_label`、`mu_top50_label`、`runtime_top100_label`、`runtime_top50_label`：按日或近 7 天的资源消耗 / 运行时长排名标签

### 3. 资源分配与浪费类

- `memory_alloc_insufficient_label`：平均分配内存小于输入规模
- `memory_waste_label`：平均分配内存显著大于输入规模

### 4. 数据倾斜 / 长尾类

- `data_skew_read_label`：最大 shuffle read 与平均 shuffle read 差距显著
- `data_skew_write_label`：最大 shuffle write 与平均 shuffle write 差距显著
- `task_duration` 长尾明显、P95 / P99 明显高于平均值时，可作为 skew / 长尾观察项

### 5. GC / Spill 类

- `full_gc_label`：major GC 占 task time 的比例过高
- `spill_disk_label`：存在明显 spill disk

### 6. 任务健康类

- `task_health_label`：当 failed / retry / top rank / memory / skew / GC / spill 等异常标签都不触发时，可判为健康任务

## 标签输出要求

- 标签只能基于直接可见的 Spark UI / 日志 / eventlog 证据。
- 如果某个标签需要更多字段才能判断，就写 `待确认`，不要猜。
- 这些标签用于辅助第 2 章证据整理，不替代第 3 章根因排序。

## 分析规则

- 只写直接看得到的证据，不把症状当根因。
- `ExecutorLostFailure`、`preempted`、`killed`、`fetch failed` 先按症状记录，除非日志直接证明是根因。
- 如果总数据量不大但某个 stage 很慢，要继续往 skew / 长尾 / 大分区 / 热点 key 方向查。
- 如果窗口排序、groupBy、join 这类 shuffle 慢点出现，要先判断是总量大还是倾斜大；不要默认都归为总量问题。
- 如果只看到平均值，不要把平均值当成现场，优先记录 P95 / P99 或最慢 task。
- 如果某个值没有直接暴露，就写 `待确认` 或 `未直接给出`。
- 如果某个环节读取行数超过 10 亿，或者输入规模超过 2T，要先标成量级异常候选，再判断是否合理。
- 如果超大 stage 数量超过 4 个，要优先提示是否需要把部分 stage 落表 / 中间表化。

## 输出

- 集群与环境信息
- Jobs / Stages / Executors / SQL 证据
- AM / driver / executor / YARN diagnostics 证据
- 每个关键 stage 的运行状态备注
- 需要在根因分析阶段继续处理的证据缺口
- 每个关键 stage 的任务数、耗时、shuffle、失败、spill、preemption、loss reason
- 和源码 stage 对齐的运行证据表

## 约束

- 不做最终根因排序。
- 不写没有直接证据支撑的因果结论。
- 如果某个值没有直接暴露，就写 `待确认` 或 `未直接给出`。
- 采集到的文本要作为证据，不要改写成摘要。
- 如果输入是 YARN application 链接，必须先采 ApplicationMaster，再进入 Spark 运行日志。
- 运行日志分析必须覆盖 `jobs`、`stages`、`executors`、`environment`、`sql`，并尽量补充 `eventlog`、AM、driver、executor、YARN diagnostics。
