# 第 4 步输出

案例：`bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline`

这里只输出第 4 步结果，第 5 步暂不执行。

## 总结

本页只保留 Spark 界面中可直接观测到的数据与源码行号；不能直接确认的原因、收益和改动统一写成“待确认”。

当前已确认热点如下：

1. 作业 16 的大窗口排序对应 `row_number()`；
2. Android 分支的 `explode_outer + groupBy + join` 链；
3. 最新价格快照的 `model, brand` 关联；
4. 末端宽汇聚 join。

未纳入排名的观察项：

- `ft_device.py:107` 周期指标宽聚合：当前日志没有单独暴露这一段的 stage 耗时、行数或 shuffle，因此不进入 top 排名，只保留为观察项。

优化顺序保持不变：

1. 先调 Spark 执行参数；
2. 再改实现方式，但不改业务逻辑；
3. 只有前两类不够时，才考虑改业务逻辑。

## 上下文快照

| 项目 | 值 |
|---|---|
| Executors | `5 x 1 core` |
| Executor memory | `4g` |
| Memory overhead | `2g` |
| Dynamic allocation | `true` |
| Shuffle partitions | `200` |
| AQE | `true` |
| Local shuffle reader | `true` |
| Upstream tables | `bmart_udl_risk.dwd_device_di`, `fmart_antifraud.dwd_antifraud_action_log_di` |
| Missing cluster keys | `spark.sql.adaptive.coalescePartitions.enabled`, `spark.sql.adaptive.skewJoin.enabled`, `spark.sql.adaptive.advisoryPartitionSizeInBytes`, `spark.sql.autoBroadcastJoinThreshold`, `spark.default.parallelism`, `spark.sql.files.maxPartitionBytes` |

## 参数调优候选

| 排名 | 参数 / 检查项 | 原因 | 查询 / 动作 | 预期收益 |
|---|---|---|---|---|
| P0-1 | `spark.sql.autoBroadcastJoinThreshold` | 价格维表关联已经走广播路径；阈值应显式配置，并结合维表大小校验。 | `grep -R "spark.sql.autoBroadcastJoinThreshold" <spark-submit-log-or-dag-config>` | 中 |
| P0-2 | `spark.sql.adaptive.skewJoin.enabled` | 作业 16 存在长尾 / 失败任务；开启倾斜处理可以降低最坏任务耗时。 | `grep -R "spark.sql.adaptive.skewJoin.enabled" <spark-submit-log-or-dag-config>` | 中高 |
| P0-3 | `spark.sql.adaptive.coalescePartitions.enabled` + `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 该作业 shuffle 很重；AQE 合并分区可以减少小任务开销。 | `grep -R "spark.sql.adaptive.coalescePartitions.enabled" <spark-submit-log-or-dag-config>` 和 `grep -R "spark.sql.adaptive.advisoryPartitionSizeInBytes" <spark-submit-log-or-dag-config>` | 中 |
| P0-4 | `spark.sql.files.maxPartitionBytes` | 原始 device 扫描覆盖了很长的历史窗口；文件分区大小会影响扫描并行度。 | `grep -R "spark.sql.files.maxPartitionBytes" <spark-submit-log-or-dag-config>` | 低中 |

## 已确认热点

| 排名 | 代码范围 | 日志证据 | 待确认原因 | 待确认收益 | 待确认改动 |
|---|---|---|---|---|---|
| 1 | `features/device/ft_device.py:38-39` `row_number()` 最新设备 | 作业 16，4.2 小时，6115 个任务，19 个失败 | 待确认 | 待确认 | 待确认 |
| 2 | `dao/device/device.py:188, 191, 193, 196, 206-207, 210, 219-220` Android `explode_outer + groupBy + join` 链 | 作业 16；shuffle 读取 1.5 TiB，写入 841.5 GiB | 待确认 | 待确认 | 待确认 |
| 3 | `ft_device.py:164` `get_device_latest_price()` + `model, brand` 关联 | 作业 15，2.2 小时，广播交换路径 | 待确认 | 待确认 | 待确认 |
| 4 | `ft_device.py:48-49` 最终宽 join（`last` + `period` + `price`） | 作业 15-16，重分支之后的宽汇聚 join | 待确认 | 待确认 | 待确认 |

## 详细内容

### 第 1 名：`features/device/ft_device.py:38-39`

**原始代码片段**

<pre style="background:#f9fafb;padding:12px;border-radius:8px;color:#9ca3af;white-space:pre-wrap;line-height:1.5">
33  win = Window.partitionBy('device_id').orderBy(func.desc('collection_date'), 'num_nulls',
34                                                func.desc('collection_time'))
35  df_last_device = df_device \
36      .withColumn('collection_date', func.to_date('collection_time')) \
37      .withColumn('num_nulls', sum(df_device[col].isNull().cast('int') for col in df_device.columns)) \
<span style="color:#b91c1c;font-weight:700">38      .withColumn('rank', func.row_number().over(win))</span>
<span style="color:#b91c1c;font-weight:700">39      .filter(func.col('rank') == 1)</span>
40      .drop('rank', 'collection_date', 'num_nulls')
</pre>

**运行指标**

- 任务数：6115
- 耗时：约 4.2 小时
- 读入 / Shuffle：`381 GiB` 输入、`1.5 TiB` shuffle read、`841.5 GiB` shuffle write
- 读行数：Spark 界面导出未直接给出

**待确认原因**

当前日志没有单独暴露这一段的耗时或行数，因此不能把它单独判成慢因。

**待确认收益**

待确认（日志未提供收益数值）。

**待确认改动**

当前日志未直接提供可验证的修改位置，因此这里不写预期改动代码。

### 第 2 名：`dao/device/device.py:188, 191, 193, 196, 206-207, 210, 219-220`

**原始代码片段**

<pre style="background:#f9fafb;padding:12px;border-radius:8px;color:#9ca3af;white-space:pre-wrap;line-height:1.5">
187  # process is_sim_card
<span style="color:#b91c1c;font-weight:700">188  df_sim_card = df_android.select(*self.pkey, func.explode_outer('is_sim_card').alias('sim_card'))</span>
<span style="color:#b91c1c;font-weight:700">189  df_sim_card = df_sim_card.withColumn('is_sim_card',</span>
<span style="color:#b91c1c;font-weight:700">190                                       func.when(func.col('sim_card').isin(['LOADED', 'READY']), 1).otherwise(0))</span>
<span style="color:#b91c1c;font-weight:700">191  df_sim_card = df_sim_card.groupBy(self.pkey).agg(func.max('is_sim_card').alias('is_sim_card'))</span>
192
<span style="color:#b91c1c;font-weight:700">193  df_android = df_android.drop('is_sim_card').join(df_sim_card, on=self.pkey, how='left')</span>
194
195  # process network type
<span style="color:#b91c1c;font-weight:700">196  df_network = df_android.select(*self.pkey, func.explode_outer('network_type').alias('network'))</span>
<span style="color:#b91c1c;font-weight:700">197  df_network = df_network.withColumn('network', func.lower('network'))</span>
...
209  # process carrier
<span style="color:#b91c1c;font-weight:700">210  df_carrier = df_android.select(*self.pkey, func.explode_outer('carrier').alias('carrier'))</span>
<span style="color:#b91c1c;font-weight:700">211  df_carrier = df_carrier.withColumn('carrier', func.lower('carrier'))</span>
...
<span style="color:#b91c1c;font-weight:700">219  df_carrier = df_carrier.select(*self.pkey, *lst_expr).groupBy(self.pkey).agg(*group_expr)</span>
<span style="color:#b91c1c;font-weight:700">220  df_android = df_android.join(df_carrier, on=self.pkey, how='left')</span>
</pre>

**运行指标**

- 任务数：作业 16 相关链路
- 耗时：待确认（当前日志未直接提供单独耗时）
- 读入 / Shuffle：`1.5 TiB` shuffle read、`841.5 GiB` shuffle write
- 读行数：Spark 界面导出未直接给出

**待确认原因**

当前日志没有单独暴露这一段的耗时或行数，因此不能把它单独判成慢因。

**待确认收益**

待确认（日志未提供收益数值）。

**待确认改动**

当前日志未直接提供可验证的修改位置，因此这里不写预期改动代码。

### 第 3 名：`ft_device.py:164`

**原始代码片段**

<pre style="background:#f9fafb;padding:12px;border-radius:8px;color:#9ca3af;white-space:pre-wrap;line-height:1.5">
155  def get_ft_device_price(self, str_anchor, df_device, df_latest_price):
156      df_device = df_device.select('device_id', 'model', 'brand')
157      df_latest_price = df_latest_price.select('model', 'brand', 'price_usd_median').withColumnRenamed(
158          'price_usd_median', 'last_price_usd')
...
<span style="color:#b91c1c;font-weight:700">164      df_ft = df_device.join(df_latest_price, on=['model', 'brand'], how='left').drop('model', 'brand')</span>
165      return df_ft
</pre>

**运行指标**

- 任务数：作业 15 的广播交换链路
- 耗时：约 2.2 小时
- 读入 / Shuffle：广播交换相关开销
- 读行数：Spark 界面导出未直接给出

**待确认原因**

当前日志没有单独暴露这一段的耗时或行数，因此不能把它单独判成慢因。

**待确认收益**

待确认（日志未提供收益数值）。

**待确认改动**

当前日志未直接提供可验证的修改位置，因此这里不写预期改动代码。

### 第 4 名：`ft_device.py:48-49`

**原始代码片段**

<pre style="background:#f9fafb;padding:12px;border-radius:8px;color:#9ca3af;white-space:pre-wrap;line-height:1.5">
<span style="color:#b91c1c;font-weight:700">48  df_ft = df_ft_device_last.join(df_ft_device_period, on=self.pkey, how='left')</span>
<span style="color:#b91c1c;font-weight:700">49  df_ft = df_ft.join(df_ft_device_price, on=self.pkey, how='left')</span>
</pre>

**运行指标**

- 任务数：作业 15-16 的收口 fan-in 段
- 耗时：待确认
- 读入 / Shuffle：待确认
- 读行数：Spark 界面导出未直接给出

**待确认原因**

当前日志没有单独暴露这一段的耗时或行数，因此不能把它单独判成慢因。

**待确认收益**

待确认（日志未提供收益数值）。

**待确认改动**

当前日志未直接提供可验证的修改位置，因此这里不写预期改动代码。

## 说明

- 输出图片位于 `step4_top5_bottlenecks.svg`。
- 列表按日志证据排序，不按代码顺序排序。
- `已确认` 项有源码和 Spark 界面导出共同支撑；`待确认` 项不直接下结论。
- Spark 界面导出里没有把所有算子的行数直接暴露出来，因此当前报告里的“读行数”统一标注为“界面导出未直接给出”，避免把推断写成事实。
- 第 5 步仍在等待用户确认。
