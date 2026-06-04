---
name: pyspark-job-optimization
description: 用于通过源码和 Spark UI 日志分析 PySpark 任务，定位瓶颈并按确认后的顺序执行优化。
---

# Pyspark Job Optimization

## 概述

当 PySpark 任务变慢时，按固定顺序处理：先读源码，再读执行证据，确认主要原因，确认可改点，最后再改代码。

## 输入约定

- 每个分析案例放在 `input/<case_name>/`。
- 源码 zip 放在 `input/<case_name>/source.zip`，或者解压到 `input/<case_name>/source/`。
- Spark UI 导出放在 `input/<case_name>/spark_ui/`。
- 集群和上游表上下文放在 `input/<case_name>/context/`（如有）。
- event log、SQL plan、截图、运行备注放在同一个 case 目录里。
- 诊断报告和变更产物输出到 `output/<case_name>/`。
- 如果 case 名不明显，就用主任务名或主表名。

## 流程

0. 采集集群资源和上游表上下文。
   使用 `scripts/collect_case_context.py` 和 `references/context-collection.md`。
   如果 Spark UI 只能通过登录态浏览器访问，先用 `scripts/collect_spark_ui_browser.py` 采集页面文本，再跑 `collect_case_context.py`。
1. 读取 PySpark 源码。
   见 `references/step-1-source-reading.md`。
2. 读取 YARN application / Spark 运行日志。
   见 `references/step-2-spark-ui-reading.md` 和 `references/input-contract.md`。
3. 结合代码路径和运行证据定位慢点。
   见 `references/step-3-root-cause-ranking.md` 和 `references/diagnostic-heuristics.md`。
4. 按影响度排序前 5 个有证据支撑的瓶颈，只给直接支持的优化建议，并等待用户确认哪些可以改。
   见 `references/step-4-optimization-proposal.md` 和 `references/final-report-spec.md`。
   第 4 步报告文字版和图片都要尽量贴近 `references/step-4-report-template.md` 与 `references/assets/step4-report-sample.svg`。
5. 执行已确认的代码变更。
   见 `references/step-5-code-change.md`。

## 扩展点

默认保持 skill 简单。如果后面某一步变大了，再把那一步拆到 `references/` 里，这个文件只保留流程入口。

## 输出原则

- 只有证据可直接观测时才写“已确认”；如果日志没有暴露，就写 `待确认`，不要猜。
- Executor 丢失、抢占、重试、Killed attempt 先当作症状，除非执行日志直接证明它是根因。
- 优先给能先去掉主瓶颈的最小改动，但没有直接证据前不要把改动写成已验证。
- 排序优化思路时，顺序固定为：先调 Spark 参数，再改实现方式，最后才考虑改业务逻辑。
- 分析源码时，额外关注任何会把一条记录展开成多条记录的操作，尤其是 `explode`、`explode_outer`、`flatMap`、`posexplode`、数组展开和展开后再聚合 / join 的链路。
- 分析源码时，额外关注中间列的下游活跃性；像 `select`、`withColumn`、`alias`、`rank`、`row_number` 这类中间结果，如果后面没进入 `filter`、`join`、`groupBy`、`agg` 或 `write`，要当作冗余列处理。
- 除了最终写入或者明确的验证动作，尽量不要在中间链路频繁使用 `show()`、`collect()`、`take()`、`toPandas()` 这类 action。
- 如果某个优化项暂时没有明确的调整代码，也必须给出大致的优化方向，不能只留空或只写 `待确认`。
