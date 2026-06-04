# 第 1 步：源码阅读

## 目的

阅读 PySpark 源码，建立后续诊断需要的代码侧上下文。

## 输入

- `input/<case_name>/source.zip` or `input/<case_name>/source/`
- PySpark job entrypoint
- Main transformation logic
- Helper modules or utilities

## 需要捕捉的内容

- Execution entry and exit points
- Main data flow
- Wide transformations and action points
- Repeated reads, joins, shuffles, and UDF usage
- Same input being built more than once
- Extra `withColumn` chains that only reshape intermediate data
- Places where data is widened before it is filtered or reduced
- Any one-to-many expansion points, especially `explode` / `explode_outer` / `flatMap` / `posexplode`, and whether they are followed by `groupBy` / `join` / `distinct`
- 中间链路里的 `show()`、`collect()`、`take()`、`toPandas()` 之类 action 点；除非是最终写入或明确验证，否则要标记为不建议频繁使用
- 输入 scan 进来的字段和分区是否在末端真的被使用；如果最后没用到，要标记出多余字段和多余分区
- 中间生成的列是否真的被下游消费；尤其是 `select` / `withColumn` / `alias` / `rank` / `row_number` 这类字段，后面如果没有进入 `filter`、`join`、`groupBy`、`agg`、`write`，要标记成冗余中间列

## 输出

- 简短代码地图
- 可能的代码热点
- 需要 Spark UI 证据确认的问题
- 需要在第 2 步补齐的证据缺口
