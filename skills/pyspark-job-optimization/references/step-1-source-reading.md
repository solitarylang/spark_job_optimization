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
- Join key 的完整诊断：
  - 是否有 `null`
  - 是否类型一致
  - 是否高倾斜
  - 是否可能发生 many-to-many join
  - join 前是否能去重或预聚合
  - join 后是否会异常放大行数
  - broadcast 表是否真的足够小
  - broadcast 后是否仍然因为主表 skew 变慢
- Any one-to-many expansion points, especially `explode` / `explode_outer` / `flatMap` / `posexplode`, and whether they are followed by `groupBy` / `join` / `distinct`
- 中间链路里的 `show()`、`collect()`、`take()`、`toPandas()`、`count()` 之类 action 点；除非是最终写入或明确验证，否则要标记为不建议频繁使用
- 输入 scan 进来的字段和分区是否在末端真的被使用；如果最后没用到，要标记出多余字段和多余分区
- 中间生成的列是否真的被下游消费；尤其是 `select` / `withColumn` / `alias` / `rank` / `row_number` 这类字段，后面如果没有进入 `filter`、`join`、`groupBy`、`agg`、`write`，要标记成冗余中间列

## 输出

- 按 Spark stage 切分的源码分析结果
- `源码 -> stage -> 指标` 对照表
- 每个 stage 对应的代码范围、算子链路和数据流说明
- 每个 stage 的潜在风险点：
  - scan 放大
  - shuffle 放大
  - 增行 / 数据膨胀
  - 数据分布不均 / skew
  - 冗余列传递
  - 重复聚合 / 重复 join
- 每个 stage 是否存在 `count()` / `show()` / `collect()` 这类 action 触发的全量执行点
- 每个 stage 是否存在长尾风险、极端大分区或热点 key 的迹象
- 每个 stage 的 `已确认` / `待确认` 标记
- 需要在第 2 步补齐的证据缺口

## 强制规则

- 源码分析必须按 Spark 任务实际 stage 拆分，而不是只按文件或函数拆分。
- 每个 stage 都要能对应到一段明确的源码算子链路。
- 看到一个算子时，必须能回指到它所在的 stage、该 stage 的运行时长、任务数、输入输出和 shuffle 指标。
- 如果某段源码还不能映射到具体 stage，就先标成 `待确认`，不要放进已确认主链路。
