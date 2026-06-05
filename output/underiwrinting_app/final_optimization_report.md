# 最终优化报告

案例：`underiwrinting_app`

## 第一章：源码分析

入口在 `source/high_risk_app.py:225-240`。主链路是 `get_dwd()` -> `get_apps_tag()` -> `get_raw()` -> 写出结果。这个任务的代码结构比较典型：前面先把 360 天窗口内的 device 数据拉出来，再在中间层做 JSON 解析、数组展开、去重和标签打分，最后再把 latest / recent 两条聚合结果合并成最终特征表。

源码分段可以这样看：

| 代码范围 | 作用 | 主要风险 |
|---|---|---|
| `source/high_risk_app.py:86-120` | 360 天窗口扫描 + `ROW_NUMBER()` | 历史扫描过长、窗口排序重 |
| `source/high_risk_app.py:124-169` | `from_json` + `explode` + `dropDuplicates` + `join` | 数据膨胀 + 去重 + join 放大 |
| `source/high_risk_app.py:176-223` | `latest_apps` / `recent_apps` 双路聚合 | 重复 shuffle 和重复聚合 |
| `source/high_risk_app.py:232-234` | `repartition(10)` 写出 | 最终写出收口（不单列风险） |

`源码 -> stage -> 指标` 的关键对应：

- `source/high_risk_app.py:86-120` -> 360 天窗口扫描 + `ROW_NUMBER()`
- `source/high_risk_app.py:124-169` -> `from_json` + `explode` + `dropDuplicates` + `join`
- `source/high_risk_app.py:176-223` -> `latest_apps` / `recent_apps` 双路聚合 + join
- `source/high_risk_app.py:232-234` -> `repartition(10)` 写出

主要风险点：

- 长历史窗口 scan
- `explode` 带来的数据膨胀
- `rank` 等中间列冗余传递
- 双路聚合重复 shuffle
- 末端如果最终表不需要全部中间字段，前面算出来的冗余列就应该尽早收掉
- `rank` 这类列如果下游没有消费，就不是“保留看着方便”，而是需要考虑删除的冗余字段

## 第二章：运行日志分析（优化前）

### 2.1 运行总览

| 维度 | 证据 | 说明 |
|---|---|---|
| Application | `application_1772593899018_1041233` | 当前分析对象 |
| 队列 | `warehouse_risk` | 运行在风险队列 |
| 资源配置 | `spark.executor.instances=18`、`spark.executor.cores=4`、`spark.executor.memory=24g`、`spark.dynamicAllocation.enabled=true`、`spark.sql.shuffle.partitions=200` | 当前执行参数 |
| 总体时长 | 约 `9h 19m` | 说明不是短任务 |
| 抢占 | `Total Number of Non-AM Containers Preempted = 76` | 资源波动明显 |
| Executors | `Active(9) / Dead(75) / Total(84)` | executor churn 很重 |

### 2.2 运行链路

```text
Environment / Jobs / Stages / Executors
        -> Stage 1：9.3 h / 6.4 TiB input / 129.2 GiB shuffle write
        -> Stage 2：360/360，且出现 killed: another attempt succeeded
        -> 结论：主慢点集中在 stage 1，且存在明显的 executor 死亡和重试信号
```

### 2.3 Stage 证据表

| Stage / 现象 | 直接证据 | 解释 |
|---|---|---|
| `stage 1` | `9.3 h`，`6.4 TiB` input，`129.2 GiB` shuffle write，`61,870` tasks，`300 failed` | 主扫描 + 排序链路，是当前全局主慢点 |
| `stage 2` | `360/360`，`killed: another attempt succeeded` | 存在重试 / 回收痕迹，但当前页面不能单独把它定成主因 |
| Executors | `Dead(75)` 明显高于 `Active(9)` | executor churn 很重 |
| AM / driver | 当前页面未进一步暴露更细诊断文本 | 仍需保留 `待确认` |

### 2.4 运行判断

- `stage 1` 是最重 stage，且规模已经达到 TiB 级输入、百 GiB 级 shuffle 写入，说明慢点不是轻量链路。
- `stage 1` 的失败 task 数高，说明这里是全局主慢点。
- `stage 2` 只说明至少有某些阶段存在重试和回收，不能直接替代主慢因判断。
- `Executors` 页 `Dead(75)` 明显高于 `Active(9)`，资源波动是真实存在的，但当前还不能把抢占/死亡单独定成唯一根因。

## 第三章：根因分析

排序后的前五个热点：

1. `get_raw()` 360 天窗口扫描 + `ROW_NUMBER()`
2. `explode` + 去重 + lending app join 链
3. `latest_apps` / `recent_apps` 双路聚合
4. 最终宽表 join 收口
5. `repartition(10)` 写出收口（仅作为结果写出边界，不单列主慢因）

当前已确认的主慢点是 `get_raw()` 对应的 `stage 1`。`explode` + `dropDuplicates` + `join` 是第二类明显放大链路。`rank` 目前按冗余中间列风险处理。

这里需要区分两层含义：

- **已确认的主慢点**：`get_raw()` 的 360 天窗口扫描和 `ROW_NUMBER()`。
- **结构性放大点**：`explode`、`dropDuplicates`、`join`、双路聚合，这些会持续把中间数据放大。
- **待确认项**：具体是 broadcast 不稳、数据倾斜、资源抢占，还是 executor 死亡带来的 shuffle 丢失，当前导出还不能单独定死。

为什么 `rank` 要单独提出来：

- 它在 `get_raw()` 中生成。
- 它在 `get_apps_tag()` 中一路透传。
- 它在最终结果构造里没有进入显式消费链。
- 所以它更像冗余字段候选，而不是主慢因本身。

## 第四章：优化方案

优先级固定为：

1. 先调 Spark 参数
2. 再改实现方式，不改业务逻辑
3. 最后才考虑业务逻辑

参数候选：

- `spark.sql.autoBroadcastJoinThreshold`
- `spark.sql.adaptive.skewJoin.enabled`
- `spark.sql.adaptive.coalescePartitions.enabled`
- `spark.sql.adaptive.advisoryPartitionSizeInBytes`
- `spark.sql.files.maxPartitionBytes`

实现方式方向：

- 收窄 `get_raw()` 的历史扫描范围
- 裁掉最终没用到的字段和分区
- `explode` 后尽早过滤和裁剪
- `rank` 若下游不使用，尽早删除
- 重复 join / groupBy 链考虑按天落中间表

这个 case 的优化方向不是单点修补，而是三层减重：

1. **先减 scan 量**：缩短历史窗口，先裁字段和分区。
2. **再减中间膨胀**：`explode` 后尽快过滤，避免把无效行一路带到后面的 join / groupBy。
3. **最后减重复计算**：如果 `latest_apps` / `recent_apps` 的结果会复用，考虑中间表；如果 `rank` 不参与最终逻辑，尽早删掉。

预期收益（估算逻辑）：

| 优化点 | 预期收益 | 估算逻辑 |
|---|---|---|
| 收窄 `get_raw()` 的 360 天窗口 | 高，约节省 3-5 小时 | 当前主慢点是 `Stage 1`，9.3 小时、6.4 TiB 输入、129.2 GiB shuffle write；缩短历史 scan 会直接削掉主耗时链路 |
| 删除 `rank` 冗余中间列 | 低，分钟级或更少 | 这列下游没有消费，优化收益主要来自减少无效列透传和列级处理 |
| `from_json` + `explode` + 去重链前置过滤 / 中间表 | 中，分钟到十几分钟 | 当前链路会放大中间行数，后续又接双路聚合和 join；若能减少 30% 左右的中间数据，收益主要体现在 shuffle 降低 |
| `latest_apps` / `recent_apps` 改成可复用中间表 | 中高，分钟到 1 小时级 | 这两条链路会重复 shuffle 和聚合，复用越充分，收益越高 |
| `repartition(10)` 写出收口 | 低，分钟级以内 | 当前导出没有把它识别为主慢因，优化主要体现在减少尾部写盘开销 |

预期修改代码（草图）：

```python
raw_df = raw_df.select(*required_cols).filter(F.col("pt_date") >= start_pt_date)
apps = (
    apps_raw
    .select("deviceId", "apps_json", *kept_cols)
    .withColumn("apps", F.from_json("apps_json", schema))
)
apps_base = apps_base.select("deviceId", "app", "pt_date").cache()
result = result.select(*final_cols)
```

## 第五章：优化效果验证（优化后）

当前没有生成优化后 `spark_ui/optimized_browser/`，也没有优化后代码快照，所以本章仍然是 `待确认`。

当前缺失的验证项包括：

- 优化后 stage 1 是否明显下降
- 优化后 shuffle write 是否下降
- 优化后 dead executor / failed task 是否减少
- 优化后的 `rank` 删除或字段裁剪是否对结果无影响

后续补跑时，这一章应该直接对比：

- 运行时长
- job / stage 数量
- shuffle 读写
- failed task
- dead executor
- 优化前后代码快照是否一致地命中了预期改动点

## 图片

- [step4_top5_bottlenecks.svg](./step4_top5_bottlenecks.svg)
- [step4_top5_bottlenecks.png](./step4_top5_bottlenecks.png)
