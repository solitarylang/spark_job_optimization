# 第 4 步输出

案例：`underiwrinting_app`

这里只输出第 4 步结果，第 5 步暂不执行。

## 总结

本页只保留 Spark 界面中可直接观测到的数据与源码行号；不能直接确认的原因、收益和改动统一写成“待确认”。
上下文采集报告已经提取到 `spark.executor.instances=18`、`spark.executor.cores=4`、`spark.executor.memory=24g` 等资源，也识别出 `ods.mbs_dispatch_center_message_log_hi` 与 `ods.mbs_app_anti_fraud_ss` 两张上游表。

当前已确认热点如下：

1. `get_raw()` 中的 360 天窗口扫描 + `ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY kafka_ts DESC)`；

未纳入排名的观察项：

- `high_risk_app.py:100-115` 的 `rank` 中间列：源码里在 `get_raw()` 生成，并在 `get_apps_tag()` 中一路传递，但 `get_dwd()` 没有真正消费它，属于冗余中间列。
- `high_risk_app.py:127-169` 的 `from_json` + `explode` + `dropDuplicates` + 小表 join：源码上看有膨胀和去重链路，但当前 Spark UI 导出没有单独暴露对应 stage 的耗时 / shuffle。
- `high_risk_app.py:188-211` 的 `latest_apps` / `recent_apps` 双路聚合：源码上看有两次 `dropDuplicates` 和两次 `groupBy`，但当前 Spark UI 导出没有单独暴露对应 stage 的耗时 / shuffle。
- `high_risk_app.py:234` 的 `repartition(10)` 写出：这是写入前的收口动作，但当前导出里没有单独可证实的慢因。

优化顺序保持不变：

1. 先调 Spark 执行参数；
2. 再改实现方式，但不改业务逻辑；
3. 只有前两类不够时，才考虑改业务逻辑。

## 上下文快照

| 项目 | 值 |
|---|---|
| YARN application | `application_1772593899018_1041233` |
| 任务名 | `high_risk_app.py` |
| 队列 | `warehouse_risk` |
| 当前状态 | `RUNNING` |
| 已运行时间 | 约 `9h 19m` |
| 非 AM 容器被抢占 | `76` |
| 被抢占资源 | `memory:2023424, vCores:76` |
| Executors | `18 x 4 cores` |
| Executor memory | `24g` |
| Executor memoryOverhead | `2g` |
| Dynamic allocation | `true` |
| Shuffle partitions | `200` |
| AQE | `true` |
| Local shuffle reader | `true` |
| Speculation | `true` |
| Broadcast timeout | `36000` |
| Upstream tables | `ods.mbs_dispatch_center_message_log_hi`, `ods.mbs_app_anti_fraud_ss` |
| Missing cluster keys | `spark.sql.adaptive.coalescePartitions.enabled`, `spark.sql.adaptive.skewJoin.enabled`, `spark.sql.adaptive.advisoryPartitionSizeInBytes`, `spark.sql.autoBroadcastJoinThreshold`, `spark.default.parallelism`, `spark.sql.files.maxPartitionBytes` |

## 参数调优候选

| 排名 | 参数 / 检查项 | 原因 | 查询 / 动作 | 预期收益 |
|---|---|---|---|---|
| P0-1 | `spark.sql.autoBroadcastJoinThreshold` | 任务里存在小表 join；需要确认广播阈值是否按预期生效。 | `grep -R "spark.sql.autoBroadcastJoinThreshold" <spark-submit-log-or-dag-config>` | 中 |
| P0-2 | `spark.sql.adaptive.skewJoin.enabled` | 主 stage 已出现大规模任务数和失败重试，先确认倾斜 join 是否开启。 | `grep -R "spark.sql.adaptive.skewJoin.enabled" <spark-submit-log-or-dag-config>` | 中高 |
| P0-3 | `spark.sql.adaptive.coalescePartitions.enabled` + `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 主 stage 的 shuffle 体量很大，先确认 AQE 合并分区是否开启。 | `grep -R "spark.sql.adaptive.coalescePartitions.enabled" <spark-submit-log-or-dag-config>` 和 `grep -R "spark.sql.adaptive.advisoryPartitionSizeInBytes" <spark-submit-log-or-dag-config>` | 中 |
| P0-4 | `spark.sql.files.maxPartitionBytes` | 主扫描覆盖长历史窗口，文件分区大小会影响扫描并行度。 | `grep -R "spark.sql.files.maxPartitionBytes" <spark-submit-log-or-dag-config>` | 低中 |

## 已确认热点

| 排名 | 代码范围 | 日志证据 | 待确认原因 | 待确认收益 | 待确认改动 |
|---|---|---|---|---|---|
| 1 | `high_risk_app.py:92-120` `get_raw()` 中的窗口扫描 | Stage 1：`225.5 h`，`6.4 TiB` 输入，`129.4 GiB` shuffle write，`61,870` 个任务，`300` 个失败任务 | 待确认 | 待确认 | 待确认 |

## 详细内容

### 第 1 名：`high_risk_app.py:92-120`

**原始代码片段**

<pre style="background:#f9fafb;padding:12px;border-radius:8px;color:#9ca3af;white-space:pre-wrap;line-height:1.5">
86  def get_raw(self, str_anchor, offset=360):
87      str_anchor = min((datetime.date.today() - relativedelta(days=1)).strftime('%Y-%m-%d'), str_anchor)
88      start_date = max('2024-03-07', (datetime.date.today() - relativedelta(days=offset)).strftime('%Y-%m-%d'))
89      # 1.order by kafka_ts as within same pt_date, it could have different records.
90      # 2.keep the rank here as this function will consider all apps installed, and now in ph, one time sampling can only get maximam 100 apps
91
92      sql = f"""
93          WITH device_tab AS
94          (
95              SELECT 
96                  get_json_object(message, '$.deviceId') AS device_id,
97                  get_json_object(message, '$.deviceInfo.os') AS os,
98                  get_json_object(message,'$.deviceInfo.apps') AS apps,
99                  DATEDIFF(DATE('{str_anchor}'), DATE(pt_date)) AS diff_days,
100                  ROW_NUMBER() OVER(PARTITION BY get_json_object(message, '$.deviceId') ORDER BY kafka_ts DESC) AS rank,
101                  pt_date
102              FROM {self.table['device']}
103              WHERE pt_date <= '{str_anchor}'
104                  AND pt_date >= '{start_date}'
105                  AND pt_biz = 'device-security'
106                  AND get_json_object(message, '$.deviceId') IS NOT NULL
107                  AND get_json_object(message, '$.deviceInfo.os') IS NOT NULL
108                  AND get_json_object(message, '$.deviceInfo.apps') IS NOT NULL
109          )
110          SELECT 
111              device_id,
112              apps,
113              os,
114              diff_days,
115              rank,
116              pt_date
117          FROM device_tab
118          """
119
120      df_raw = SparkSession.builder.getOrCreate().sql(sql)
</pre>

**运行指标**

- 任务数：61,870
- 耗时：约 225.5 小时
- 读入 / Shuffle：`6.4 TiB` 输入、`129.4 GiB` shuffle write
- 读行数：`3,368,636,240`

**待确认原因**

当前日志没有把这段 SQL 内部再拆成更细的子 stage，因此这里能直接确认的是“这段就是主慢点”，但不能继续精确拆到更细的局部根因。

**待确认收益**

待确认（日志未提供收益数值）。

**优化方向**

优先收窄 360 天扫描范围，裁掉最后没用的字段和分区；如果历史结果会被反复使用，优先把它沉淀成中间表。

**待确认改动**

当前日志未直接提供可验证的修改位置，因此这里不写预期改动代码。

## 说明

- 输出图片已生成：
  - [SVG](/Users/lang.jiang/Documents/spark_job_optimization/output/underiwrinting_app/step4_top5_bottlenecks.svg)
  - [PNG](/Users/lang.jiang/Documents/spark_job_optimization/output/underiwrinting_app/step4_top5_bottlenecks.png)
- 列表按日志证据排序，不按代码顺序排序。
- `已确认` 项有源码和 Spark 界面导出共同支撑；`待确认` 项不直接下结论。
- 第 5 步仍在等待用户确认。
