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

## 需要重点采集的运行信息

- Application Overview：application id、name、user、queue、状态、开始结束时间、diagnostics
- Environment：Spark 版本、Spark 配置、执行参数、dynamic allocation、shuffle partitions、broadcast threshold、AQE、speculation
- Jobs：job 数量、长耗时 job、失败 / 重试 job、重复提交痕迹
- Stages：stage 级耗时、task 数、input / output、shuffle read / write、失败 reason、retry / skipped / spill
- Executors：active / dead / total、负载是否均衡、GC / spill / shuffle / task time、loss reason
- SQL：query plan、join / exchange / window / broadcast / stage 对应关系
- EventLog：原始 stage / task / job 证据
- AM / driver / executor / YARN diagnostics：`OOM`、`Killed by YARN`、`preempted`、`node lost`、`fetch failed`、`file not found`、`disk error`、`exit code`

## 分析规则

- 只写直接看得到的证据，不把症状当根因。
- `ExecutorLostFailure`、`preempted`、`killed`、`fetch failed` 先按症状记录，除非日志直接证明是根因。
- 如果总数据量不大但某个 stage 很慢，要继续往 skew / 长尾 / 大分区 / 热点 key 方向查。
- 如果只看到平均值，不要把平均值当成现场，优先记录 P95 / P99 或最慢 task。
- 如果某个值没有直接暴露，就写 `待确认` 或 `未直接给出`。

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
