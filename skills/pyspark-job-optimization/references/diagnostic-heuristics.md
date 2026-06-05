# Diagnostic Heuristics

## Analysis Rules

- 先找证据强的现象，再反推代码。
- 不要把 `ExecutorLostFailure`、`preempted`、`killed` 直接当根因，它们通常是慢任务的外显结果。
- 优先判断数据量放大点、shuffle 放大点、重复扫描点，而不是先猜资源配置。
- 先看数据分布，不要只看总量；总数据量不大但 stage 很慢时，优先怀疑 skew。
- 判断 stage 是否有长尾，重点看 task duration 的 P95 / P99，而不是只看平均值。
- 如果 shuffle partition 出现极端大分区，或者 join key / groupBy key 的 top key 分布极端不均，要优先当作 skew 问题处理。
- 先给低风险优化，再给结构性改造。
- 除了最终写入或者明确的验证动作，尽量不要在中间链路频繁使用 `show()`、`collect()`、`take()`、`toPandas()` 这类 action；如果必须使用，也要确认它们不会成为新的性能瓶颈。
- `count()` 不要在主链路里频繁使用。它通常会触发完整 DAG 执行；如果只是验证是否为空，优先考虑 `limit(1)`、写入后看产出分区、依赖元数据或上游统计。
- 如果 `count()` 是业务逻辑必需，优先考虑落中间表或复用缓存，避免反复触发全量计算。
- 先检查输入 scan 进来的字段和分区，确认末端是否真的需要；如果最后没有使用，就尽早裁掉多余字段和多余分区，避免前面读得多、最后用得少。
- 分析代码时，要特别盯住一切“一条变多条”的增行操作，尤其是 `explode`、`explode_outer`、`flatMap`、`posexplode`、数组展开、map 展开、先展开再 `groupBy` / `join` 的链路；这类位置要默认当作数据膨胀点处理。
- 分析代码时，也要检查中间列是否真的有下游消费；`select`、`withColumn`、`alias`、`rank`、`row_number`、`dense_rank` 之类中间结果，如果后面没有进入 `filter`、`join`、`groupBy`、`agg`、`write`，就按冗余中间列处理。

## Stage Symptoms

- `Shuffle Read` / `Shuffle Write` 很大且 stage 很慢:
  - 优先怀疑 `groupBy`、`join`、`window`、`distinct`、`sort`、`repartition`
- 总数据量不大但 stage 仍然很慢:
  - 优先怀疑 skew、极端大分区、热点 key
- `1/1` 或少量 task 的 stage 很慢:
  - 常见于单点重计算、单分区数据过大、窗口排序、单表大聚合
- `broadcast exchange` 很快但后续 stage 很慢:
  - 问题通常不在广播本身，而在主表 shuffle 或后续聚合
- `killed: another attempt succeeded` 很多:
  - 常见于慢 stage、skew、AQE 重分区或资源抢占

## Operator Mapping

- `groupBy / agg`
  - 高概率触发 shuffle
- `join`
  - 小维表未 broadcast 会放大 shuffle
  - join key 分布不均时，优先怀疑 skew
- `Window.row_number / rank / dense_rank`
  - 在大分区上通常很重
- `explode / explode_outer`
  - 会放大中间行数
  - 如果后面还接 `groupBy`、`join`、`distinct`，要额外警惕二次放大和 shuffle 膨胀
- `select / drop / withColumn`
  - 如果只是为了保留最终会用到的字段，优先尽早裁剪无用列，避免把无关字段一路带到末端
  - 任何新生成的中间列都要追踪下游使用情况，避免“算出来但没用”的列一直传递到末端
- `distinct / dropDuplicates`
  - 通常有高 shuffle 成本
- `collect / toPandas / take`
  - 如果只是拿小结果集，优先改成广播、缓存、落中间表或静态配置；不要在主链路里反复触发
- `count`
  - 只在明确需要全量计数时使用；如果只是判断是否为空，优先用 `limit(1)` 或元数据
  - 如果全链路里反复出现，先把它当作完整 DAG 触发点来审视
- `skew / 长尾`
  - 平均值不可信，P95 / P99 task 才更接近问题现场
  - 如果 task duration 长尾明显，先怀疑数据倾斜，再看资源和 shuffle
- `persist / cache`
  - 只在会被重复使用且体量可控时考虑

## Reasoning Checklist

1. 这一步是在“增行”还是“减行”？
2. 这一步是否引入 shuffle？
3. 这一步的输入是否被重复读取？
4. 这一步是否有小表可以 broadcast？
5. 这一步的缓存是否明显大于可用内存？
6. 这一步是否只是为了拿一个很小的结果集？
7. 这一步是否能先预聚合、先过滤、先压缩粒度？

## 优化排序

- `P0`
  - 优先调 Spark 执行参数
  - 去掉重复扫描
  - 收紧缓存
  - broadcast 小表
  - 避免多余 `collect` / `show` / `take` / `toPandas` / `count`
- `P1`
  - 先做数据缩减，再做窗口 / 排序
  - 拆分重 join，优先把过滤前置
  - 识别并拆掉 `explode -> groupBy` 的膨胀链路
  - 减少中间列和重复 `withColumn`
  - 检查末端是否有多余字段和多余分区，能删就尽早删掉
- `P2`
  - 当单次 scan 量过大时，优先考虑构建中间表，把重扫描变成可复用的落地结果
  - 当 join 链路过长或重复 join 很多时，优先考虑按天构建中间表，提前把可复用的宽表或聚合结果沉淀下来
  - 重构分区策略和表粒度
  - 改写核心数据模型

## 优化策略顺序

1. 只调整 Spark 执行参数。
2. 修改实现方式，但不改业务逻辑。
3. 只有在前两类方案不足以解决问题时，才考虑修改业务逻辑。
