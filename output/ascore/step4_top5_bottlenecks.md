# ascore 第 4 章：优化方案摘要

配套图片：

- SVG: `output/ascore/step4_top5_bottlenecks.svg`
- PNG: `output/ascore/step4_top5_bottlenecks.png`

## 总结

当前任务是一个 PySpark 风险评分链路，主流程为历史事实扫描、月度特征构造、WOE 转换、模型预测和 Hive 写入。  
源码侧已经能确认存在大量历史扫描、重复分支、过度特征展开和一次额外的 `count()` 全量触发；浏览器采集的 Spark UI 进一步显示，这条链路的重压点集中在：

- `count()` 触发的 1.7605 TiB 级别全量读取
- 多个 `saveAsTable` 长链路 stage，最长达到 1.6 h
- 74 个 executor 中有 64 个进入 Dead 状态
- 总 shuffle read 达到 5 TiB，总 shuffle write 达到 2.4 TiB

## 上下文快照

- 应用链接：`application_1772593899018_1055219`
- 源码入口：`input/ascore/source/loanstatus_offline_prod_hdfs.py`
- Spark UI 应用名：`zx_prod_loanstatus`
- Total Uptime：`4.5 h`
- Scheduling Mode：`FIFO`
- Executors：`Active(10) / Dead(64) / Total(74)`
- 运行指标：
  - `Total task time = 276.2 h`
  - `Total shuffle read = 5 TiB`
  - `Total shuffle write = 2.4 TiB`
  - `Completed Jobs = 43`

## 参数调优候选

### P0

1. 先处理 `sdf.count()` 前的全量触发。
2. 再处理 `sdf_ft_loan_appl` 整条分支。

### P1

1. 将 `sdf_credit_all` 先落中间表，再复用给多个分支。
2. 若最终仍保留大规模 `groupBy / pivot`，再根据 UI 证据确认 `shuffle partitions` 和 executor 资源。

### P2

1. 若后续补到 skew / 长尾证据，再考虑 salting 或按 key 预聚合。

## 已确认热点

1. `sdf.count()` 在最终写表前再次触发全量执行，Spark UI 上对应的 `count` job 达到 `19 min`，并读取了 `1.7605 TiB` 级别输入。
2. `sdf_loan_appl` 整条分支是高成本候选，且最终没有进入 `ft_keep`。
3. `sdf_client_dpd_monthly` 及其后续特征展开，生成了大量最终未保留的列。
4. `sdf_loan_disbursement_monthly` 及其后续特征展开，同样存在大量最终未保留列。
5. `sdf_credit_all` 在多条分支里复用，但没有任何缓存或中间表承接。

## 详细内容

### 1. `sdf_loan_appl` 整条分支

- 原始代码片段：`input/ascore/source/loanstatus_offline_prod_hdfs.py:332-394`
- 问题点：
  - 从 `dwd_loan_application_dc` 扫描 130 天历史
  - 通过 `tt2.pt_date >= tt1.start_date and tt2.pt_date < tt1.end_date` 做日期展开
  - 再 `groupBy("client_no").pivot("pt_date")`
  - 最终 `ft_keep` 中没有任何 loan application 特征
- 日志证据：
  - `Jobs` 页显示前置 `pivot` 链路处理了 `135` 个分区路径
  - `Stages` 页对应 stage 2 的运行时长为 `9.5 min`，任务数为 `536/536`
- 待确认原因：
  - 从源码看，这条分支属于纯计算候选，当前输出链路未使用
- 预期收益：
  - 这条分支可直接删除，理论上可去掉一整条扫描、展开、shuffle、pivot 链路
  - 由于最终未进入输出，收益可视为 `高`
- 优化方向：
  - 直接删除，或者至少延后到确认需要后再保留
- 待确认改动：
  - 删除 `sdf_loan_appl`、`sdf_appl_client`、`sdf_ft_loan_appl`
- 预期修改代码（草图）：
```python
# 如果模型和最终输出都不需要 loan application 特征，整条分支删除
# sdf_loan_appl = ...
# sdf_appl_client = ...
# sdf_ft_loan_appl = ...
```

### 2. `sdf_client_dpd_monthly` 的过度特征展开

- 原始代码片段：`input/ascore/source/loanstatus_offline_prod_hdfs.py:106-149, 262-326`
- 问题点：
  - 先对 130 天 `channel_loan_credit_risk_tmp` 做月度聚合
  - 再生成 `label_*`、`ft_has_*`、`ft_count_*`、`ft_pct_*` 等大量列
  - 最终 `ft_keep` 只保留 8 个 DPD 相关 raw 特征
- 日志证据：
  - `Stages` 页中有多个 `count` / `saveAsTable` 链路反复处理 `200`、`536`、`1446`、`11251` 级别的任务集合
  - 其中一个 `count` job 运行了 `19 min`，读取 `1.7605 TiB` 输入
- 待确认原因：
  - 不是单纯“慢”，而是大量中间列生成后被末端裁掉
- 预期收益：
  - 约 132 个 DPD 候选特征里仅保留 8 个，92% 以上为冗余展开
  - 若只生成保留列，可明显减少宽表构造和列级表达式计算
- 优化方向：
  - 只计算最终需要的 DPD 特征
  - 删除中间未消费列的计算逻辑，而不是只在结果阶段 drop
- 待确认改动：
  - 收敛 `generate_*_dpd` 的特征集合，仅保留最终 `ft_keep` 需要的列
- 预期修改代码（草图）：
```python
required_dpd_cols = [
    "ft_count_xdpd_sum_2m",
    "ft_count_xdpd_max_1m",
    "ft_count_7dpd_sum_3m",
    "ft_pct_xdpd_change_3m",
    "ft_count_15dpd_sum_3m",
    "ft_count_7dpd_sum_1m",
    "ft_count_xdpd_change_3m",
    "ft_count_xdpd_change_ratio_1m",
]
# 仅生成 required_dpd_cols 对应表达式
```

### 3. `sdf_loan_disbursement_monthly` 的过度特征展开

- 原始代码片段：`input/ascore/source/loanstatus_offline_prod_hdfs.py:520-558`
- 问题点：
  - 从同一个 `sdf_credit_all` 再做一次 `dropDuplicates + groupBy + pivot`
  - 后续生成 24 个 disbursement 候选特征
  - 最终只保留 2 个 raw 特征
- 日志证据：
  - `saveAsTable` 链路里出现了 `27 min`、`44 min`、`1.2 h`、`1.6 h` 级别的长 stage
  - 这些 stage 的任务数达到 `11,251` 级别，并伴随失败任务
- 待确认原因：
  - 中间计算量明显大于最终保留量
- 预期收益：
  - 可减少大量列级表达式和后续宽表 join 负担
  - 这部分收益可视为 `中-高`
- 优化方向：
  - 只生成 `ft_keep` 中真正需要的 disbursement 特征
- 待确认改动：
  - 收敛 `sdf_ft_loan_disbursement` 的 feature list
- 预期修改代码（草图）：
```python
required_disbursement_cols = [
    "ft_disbursement_amount_max_2m",
    "ft_disbursement_count_max_3m",
]
```

### 4. `sdf_credit_all` 的重复复用但未缓存

- 原始代码片段：`input/ascore/source/loanstatus_offline_prod_hdfs.py:74-114, 520-529`
- 问题点：
  - 同一个 130 天 base scan 派生出 DPD 分支和 disbursement 分支
  - 源码里没有 `cache/persist` 或中间表承接
- 日志证据：
  - `Executors` 页显示 `Dead(64) / Total(74)`，总 shuffle read 达到 `5 TiB`
  - 这类重复复用如果不固化，会被后续链路反复放大
- 待确认原因：
  - 这会放大重复读和重复计算
- 预期收益：
  - 若该 base scan 必须复用，落中间表或持久化后可减少重复读取
  - 收益通常是 `中`
- 优化方向：
  - 若两条分支都保留，考虑在 base scan 后做中间表 / checkpoint / persist
- 待确认改动：
  - 在 `sdf_credit_all` 后增加中间表化或持久化策略
- 预期修改代码（草图）：
```python
sdf_credit_all = spark.sql(...).withColumn("cur_balance", ...)
sdf_credit_all = sdf_credit_all.persist()
```

### 5. `sdf.count()` 导致最终 DAG 再跑一遍

- 原始代码片段：`input/ascore/source/loanstatus_offline_prod_hdfs.py:759-760`
- 问题点：
  - `sdf = sdf.withColumn("client_count", F.lit(sdf.count()))`
  - `count()` 是 action，会触发完整 DAG
  - 后面还有 `sdf.write...saveAsTable(...)`，通常意味着再跑一次写入 DAG
- 日志证据：
  - `count` job 21 运行了 `19 min`，读取了 `1.7605 TiB`
  - 后续 `saveAsTable` 链路仍然继续占用 `27 min`、`44 min`、`1.2 h`、`1.6 h`
- 待确认原因：
  - 这会造成一次额外的全量执行
- 预期收益：
  - 若把 `client_count` 前置到更早的稳定阶段，或先缓存 final dataframe，可避免重复触发
  - 收益可视为 `高`
- 优化方向：
  - 在较早的稳定阶段计算 row count，或者先 persist 再 count/write
- 待确认改动：
  - 将 `client_count` 计算前移，或显式缓存 final df
- 预期修改代码（草图）：
```python
# 方案 A：前移到 row count 稳定的更早阶段
client_count = sdf_sample_loanstatus.count()
sdf = sdf.withColumn("client_count", F.lit(client_count))

# 方案 B：如果必须基于 final df，先 persist 再 count/write
# sdf = sdf.persist()
```

## 说明

- 这份摘要只基于源码和浏览器采集到的 Spark UI 可直接确认的事实整理。
- 如果后续补到 SQL / eventlog / task detail，可以把 `待确认` 的项继续收紧。
