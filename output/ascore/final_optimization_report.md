# 最终优化报告

案例：`ascore`

## 第一章：源码分析

### 1.1 输入与执行路径

源码入口为 `input/ascore/source/loanstatus_offline_prod_hdfs.py`。  
主流程如下：

1. 计算 `feature_date` 和一组 month-end 日期。
2. 读取 `channel_loan_credit_risk_tmp`，构造 DPD 月度特征。
3. 构造贷款申请特征、放款特征、样本和 MOB。
4. 读取 WOE 模型并做 WOE 转换。
5. 加载 GLR 模型和 VectorAssembler，生成预测与 logodds。
6. 组装输出并写入 Hive。

Spark 会话配置里可以直接确认：

- `spark.executor.instances = 32`
- `spark.executor.cores = 4`
- `spark.executor.memory = 16g`
- `spark.executor.memoryOverhead = 2g`
- `spark.dynamicAllocation.enabled = False`
- `spark.dynamicAllocation.maxExecutors = 32`
- `spark.sql.shuffle.partitions = 200`
- `spark.sql.adaptive.enabled = true`
- `spark.sql.adaptive.localShuffleReader.enabled = true`

### 1.2 主要逻辑与数据流

#### 阶段 A：基础逾期事实扫描与月度聚合

- 代码范围：`74-149`
- 输入表：`bmart_udl_risk.channel_loan_credit_risk_tmp`
- 处理：
  - 过滤 130 天历史窗口
  - 计算 `cur_balance`
  - 以 `client_no / loan_no / pt_date` 做月度聚合
  - 以 `client_no / pt_date` 做月度余额聚合
  - 生成 `label_*`
  - `pivot(pt_date, values=dates_of_interest)`
- 风险：
  - 130 天历史扫描窗口长
  - `groupBy` / `pivot` 都会触发 shuffle
  - 同一个 base scan 后面还会继续被多分支复用

#### 阶段 B：DPD 特征展开

- 代码范围：`152-326`
- 处理：
  - `generate_ever_dpd`
  - `generate_maximum_dpd`
  - `generate_minimum_dpd`
  - `generate_sum_dpd`
  - `generate_change_dpd`
  - `generate_chg_ratio_dpd`
  - 构造 `sdf_client_ever_dpd`、`sdf_client_count_dpd`、`sdf_client_pct_dpd`
- 风险：
  - 候选特征极多，但最终只保留少量 raw 特征
  - 大量中间列在末端被裁掉，属于冗余计算

#### 阶段 C：贷款申请分支

- 代码范围：`332-394`
- 处理：
  - 扫描 `fmart_loan.dwd_loan_application_dc`
  - 与 `dws.t80_dim_time_cs_d` 做日期展开
  - 过滤失败申请
  - `groupBy(client_no).pivot(pt_date)`
- 风险：
  - 日期展开会放大行数
  - 该分支最终没有进入 `ft_keep`

#### 阶段 D：放款分支

- 代码范围：`520-558`
- 处理：
  - 从 `sdf_credit_all` 生成放款月度特征
  - `dropDuplicates`
  - `groupBy(client_no).pivot(pt_date)`
  - 展开 count / amount 特征
- 风险：
  - 同样属于重复扫描后的宽表展开
  - 最终保留的 raw 特征很少

#### 阶段 E：样本构造与最终合并

- 代码范围：`562-650`
- 处理：
  - 扫描 `fmart_loan.dwd_loan_accounting_df`
  - 过滤 feature_date
  - 计算 `client_start_date`
  - 计算 `mob`
  - 读取上一月末 DPD
  - 构造 `seg_obs`
  - 与 DPD / loan application / disbursement 特征合并
  - 仅保留 `ft_keep`
  - `fillna(0)`
- 风险：
  - 多表 join 链较长
  - 如果上游某条分支不需要，最终 DAG 仍会被拖长

#### 阶段 F：WOE / 模型 / 输出

- 代码范围：`660-825`
- 处理：
  - `WOEtransformer.load`
  - `WOEtransformer.transform`
  - 加载 GLR 模型与 VectorAssembler
  - `predict`
  - `logodds`
  - `sdf.count()` 生成 `client_count`
  - `select` 组装 JSON
  - `drop partition + saveAsTable`
- 风险：
  - `count()` 是 action，会触发完整 DAG
  - 写表前没有看到缓存 / 中间表承接，容易导致 count 和 write 各跑一遍 lineage

### 1.3 按 stage 拆分的源码对照

> 说明：Spark UI 现在可以通过登录态 Chrome 访问，以下 stage 级指标均来自浏览器采集页面；如果代码段与 runtime 没法一一对应，仍以 `待确认` 标注。

| 逻辑阶段 | 代码范围 | 关键算子 / 动作 | 对应运行证据 |
|---|---|---|---|
| A | 74-149 | `spark.sql`、`groupBy`、`pivot`、`max/avg` | Stage 65 / Stage 76 的大 shuffle 链条 |
| B | 152-326 | 多组 feature 生成、`select`、`join` | Stage 65 / Stage 76 / Stage 116 的宽链路 |
| C | 332-394 | 申请表扫描、日期展开、`groupBy`、`pivot` | Stage 0 / Stage 2 |
| D | 520-558 | 再次复用 `sdf_credit_all`、`dropDuplicates`、`groupBy`、`pivot` | Stage 116 |
| E | 562-650 | accounting 扫描、`groupBy`、`join`、`dropDuplicates` | Stage 76 / Stage 116 |
| F | 660-825 | WOE / model / `count()` / `write` | Stage 47 / Stage 65 / Stage 76 |

### 1.4 潜在风险与增行点

1. `channel_loan_credit_risk_tmp` 的 130 天历史扫描。
2. `sdf_loan_appl` 的日期维表展开再过滤逻辑。
3. `groupBy + pivot` 在 DPD / 放款 / 申请链路里反复出现。
4. `sdf_credit_all` 被多个分支复用，但没有 cache / persist / 中间表。
5. `sdf.count()` 位于最终输出前，会额外触发一次完整 DAG。
6. 很多中间特征最终没有进入 `ft_keep`，属于冗余列计算。

## 第二章：运行日志分析（优化前）

### 2.1 集群与环境信息

浏览器采集到的关键环境信息：

- `spark.app.name = zx_prod_loanstatus`
- `spark.app.id = application_1772593899018_1055219`
- `spark.executor.instances = 32`
- `spark.executor.cores = 4`
- `spark.executor.memory = 16g`
- `spark.executor.memoryOverhead = 2g`
- `spark.dynamicAllocation.enabled = False`
- `spark.sql.shuffle.partitions = 200`
- `spark.sql.adaptive.enabled = true`
- `spark.sql.adaptive.localShuffleReader.enabled = true`
- `spark.scheduler.mode = FIFO`
- `spark.eventLog.enabled = true`
- `spark.shuffle.service.enabled = true`

应用总信息：

- Total Uptime：`4.5 h`
- Completed Jobs：`43`
- Scheduling Mode：`FIFO`

### 2.2 Jobs / Stages / Executors

#### Jobs

Jobs 页展示的最重要事实是：

- Job 42：`saveAsTable`，`53 min`，`200/200 (38215 skipped)`
- Job 38：`saveAsTable`，`1.2 h`，`200/11451`
- Job 35：`saveAsTable`，`2.3 h`，`200/200 (80 failed) (11251 skipped)`
- Job 29：`saveAsTable`，`44 min`，`11251/11251 (116 failed)`
- Job 21：`count`，`19 min`，`200/200 (14699 skipped)`，`1760.5 GiB` input

这说明最终链路不是单次写表，而是存在多次大规模 `count/saveAsTable` 触发。

#### Stages

最关键的 stage 指标如下：

| Stage | Duration | Tasks | Input / Shuffle Read | Output / Shuffle Write | 备注 |
|---|---:|---:|---:|---:|---|
| 65 | `33.0 h` total task time | `11251` completed, `116 failed` | `739.2 GiB` input | `1314.0 GiB` shuffle write | 任务非常多，分布较均匀，偏大总量 |
| 76 | `51.4 h` total task time | `200` completed, `80 failed` | `1314.0 GiB` shuffle read | `5.0 GiB` shuffle write | 读放大明显，失败任务很多 |
| 116 | `23.3 h` total task time | `200` completed | `446.5 GiB` shuffle read | `254.1 MiB` output | 典型的大 shuffle read 阶段 |
| 47 | `19 min` | `200` completed | `1760.5 GiB` input | `11.5 KiB` output | `count()` 全量触发证据 |
| 2 | `9.5 min` | `536` completed | `1593.9 MiB` input | `27.9 KiB` output | 申请表日期展开后的 pivot |

Task 分布显示：

- Stage 65：`median 10 s`，`max 42 s`
- Stage 76：`median 13 min`，`max 42 min`
- Stage 116：`median 4.7 min`，`max 28 min`

这几个分布都没有出现特别夸张的单点长尾，说明主要问题更偏**总量 / 宽 shuffle / 重复触发**，而不是单纯的极端 skew。

#### Executors

- `Total(74)`
- `Active(10)`
- `Dead(64)`
- `Task Time = 276.2 h`
- `Shuffle Read = 5 TiB`
- `Shuffle Write = 2.4 TiB`
- `Failed Tasks = 5`

这说明应用过程中 executor churn 很重，但当前 UI 没有直接暴露 loss reason，所以不能直接下死结论说是 OOM、preempted 还是 node lost。

### 2.3 SQL / EventLog / AM / Driver 证据

当前浏览器采集页里没有更细的 SQL plan / eventlog / AM / driver 原文，因此：

- SQL 执行计划：`待确认`
- EventLog：`待确认`
- AM / Driver / Executor 的具体错误栈：`待确认`

### 2.4 长尾 / skew / 失败链路

可以直接确认的事实：

- Stage 65 / 76 / 116 都是大 shuffle 链路。
- Stage 65 的 task 分布比较均匀，`max 42 s`，不像明显 skew。
- Stage 76 / 116 也没有出现极端离群任务，更多是**大体量 shuffle 读取**。
- Stage 76 和多个 saveAsTable stage 里有明显 failed task。

因此当前更合理的判断是：

1. 首要问题是大数据量和多次大 shuffle。
2. 其次是 count / write 造成的重复执行。
3. Skew 没有被直接证明，但仍然是待确认项。

## 第三章：根因分析

### 3.1 证据对齐

把源码和运行证据对齐后，最明确的关系是：

- `sdf_loan_appl` 对应 Stage 0 / 2。
- `sdf.count()` 对应 Stage 47。
- DPD / disbursement 相关宽链路对应 Stage 65 / 76 / 116。
- 最终写表链路对应多个 `saveAsTable` job（35 / 38 / 42）。

### 3.2 前五个主要瓶颈

#### 瓶颈 1：贷款申请分支是高成本且最终未消费的计算链

- 代码范围：`332-394`
- 运行证据：
  - Stage 0：`20 s`
  - Stage 2：`9.5 min`
  - `536` 个 task
  - 仅 `27.9 KiB` 级别输出
- 结论：
  - 这条分支完成了日期展开和 pivot，但最终没有进入 `ft_keep`
  - 属于“算了很多，最后没用上”的冗余链路

#### 瓶颈 2：`sdf.count()` 触发全量 lineage

- 代码范围：`759-760`
- 运行证据：
  - Stage 47：`19 min`
  - `200/200`
  - `1760.5 GiB` input
  - `11.5 KiB` output
- 结论：
  - `count()` 在 final dataframe 上执行，直接把完整 DAG 跑了一遍
  - 这是一个明确的全量触发点

#### 瓶颈 3：DPD 特征展开过度，生成量远大于保留量

- 代码范围：`106-149, 152-326`
- 运行证据：
  - Stage 65：`33.0 h` total task time
  - `11251` tasks
  - `739.2 GiB` input
  - `1314.0 GiB` shuffle write
  - `116 failed`
- 结论：
  - 约 132 个 DPD 候选列，最终只保留 8 个 raw 特征
  - 中间特征计算量明显高于最终保留量

#### 瓶颈 4：`sdf_credit_all` 多分支复用但未缓存

- 代码范围：`74-114, 520-529`
- 运行证据：
  - Stage 65 / 76 / 116 都处在重 shuffle 链路上
  - Executors 页显示 `64 dead`，总 shuffle read/write 达到 `5 TiB / 2.4 TiB`
- 结论：
  - 同一 base scan 同时服务 DPD 和 disbursement 分支，却没有中间表或 persist
  - 会把重复读写和重复 shuffle 放大

#### 瓶颈 5：放款分支同样有明显的中间列膨胀

- 代码范围：`520-558`
- 运行证据：
  - Stage 116：`23.3 h` total task time
  - `200` tasks
  - `446.5 GiB` shuffle read
  - `254.1 MiB` output
- 结论：
  - 这条分支也是从复用的 base scan 上继续做 `dropDuplicates + pivot`
  - 最终保留的 raw 特征很少，但中间链路依然很重

### 3.3 已确认 / 待确认

- 已确认：
  - 贷款申请分支是未消费的高成本链路
  - `count()` 触发全量执行
  - DPD / 放款分支都存在大规模中间展开
  - executor churn 明显
- 待确认：
  - 具体哪一个 executor loss reason 导致 dead executor
  - 是否存在真实 skew / 热点 key
  - `SQL plan` 中各 join 是否被 AQE 重写

## 第四章：优化方案

配套图片：

- SVG: `output/ascore/step4_top5_bottlenecks.svg`
- PNG: `output/ascore/step4_top5_bottlenecks.png`

### 4.1 优化优先级

1. 先调 Spark 参数：当前只有候选项，没有直接证据证明某个参数一定要改。
2. 再改实现方式：这一步最明确，优先做。
3. 最后才考虑业务逻辑：只在确认历史链路必须重复解析时再动。

### 4.2 参数调优候选

- `spark.sql.shuffle.partitions`
- `spark.sql.adaptive.coalescePartitions.enabled`
- `spark.sql.adaptive.skewJoin.enabled`
- `spark.sql.autoBroadcastJoinThreshold`

这些项都需要结合优化后 / 更细 SQL plan 再确认，当前只能作为检查项。

### 4.3 实现方式优化

#### 优化 1：删除 `sdf_loan_appl` 整条分支

- 目标：去掉一个已经确认的高成本但未消费分支。
- 代码草图：
```python
# 删除整条 loan application 分支
# sdf_loan_appl = ...
# sdf_appl_client = ...
# sdf_ft_loan_appl = ...
```
- 预期收益：高，能直接减少一次 536 task 的 pivot 链路。

#### 优化 2：把 `count()` 前移或缓存 final dataframe

- 目标：避免 `count()` 把整条最终 DAG 重新跑一遍。
- 代码草图：
```python
client_count = sdf_sample_loanstatus.count()
sdf = sdf.withColumn("client_count", F.lit(client_count))
# 或者：
# sdf = sdf.persist()
# client_count = sdf.count()
```
- 预期收益：高，直接减少一次全量执行。

#### 优化 3：收敛 DPD 特征生成范围

- 目标：只生成最终 `ft_keep` 需要的 DPD 特征。
- 代码草图：
```python
required_dpd_cols = [
    "ft_count_xdpd_sum_2m",
    "ft_count_xdpd_max_1m",
    "ft_disbursement_amount_max_2m",
    "ft_disbursement_count_max_3m",
    "ft_count_7dpd_sum_3m",
    "ft_pct_xdpd_change_3m",
    "ft_count_15dpd_sum_3m",
    "ft_count_7dpd_sum_1m",
    "ft_count_xdpd_change_3m",
    "ft_count_xdpd_change_ratio_1m",
]
```
- 预期收益：中高，减少大量列级表达式和后续宽表处理。

#### 优化 4：复用或中间表化 `sdf_credit_all`

- 目标：避免同一个 130 天 base scan 在多分支里重复计算。
- 代码草图：
```python
sdf_credit_all = spark.sql(...).withColumn("cur_balance", ...)
sdf_credit_all = sdf_credit_all.persist()
```
- 预期收益：中，减少重复读写和重复 shuffle。

#### 优化 5：收敛放款分支的候选特征

- 目标：只保留最终 `ft_keep` 需要的 disbursement 特征。
- 代码草图：
```python
required_disbursement_cols = [
    "ft_disbursement_amount_max_2m",
    "ft_disbursement_count_max_3m",
]
```
- 预期收益：中，减少宽表展开和下游 join 压力。

### 4.4 业务逻辑优化

当前没有足够运行证据要求改业务逻辑。  
如果后续确认同一批历史数据在不同日期都会重复解析，再考虑：

- 增量化
- 按天沉淀中间表
- 解析结果复用

### 4.5 待确认项

- Executor 死亡的真实原因
- SQL plan 是否存在可广播维表
- DPD / 放款链路中是否存在隐藏 skew

## 第五章：优化效果验证（优化后）

### 5.1 优化后运行证据

`待确认`。  
当前还没有优化后的 Spark UI / eventlog。

### 5.2 优化前后对比

`待确认`。  
需要重新采集：

- 总运行时长
- job 数
- stage 耗时
- shuffle read / write
- failed / retry
- dead executor 数量

### 5.3 优化前后代码快照对比

`待确认`。  
当前只有优化前代码快照。

### 5.4 效果结论

当前这版分析已经可以确认：  
真正的问题不是“某个算子单点很慢”，而是**多个高成本分支 + 大量中间特征展开 + `count()` 的全量重跑 + 最终写表的重 shuffle** 叠加在一起，导致整条链路膨胀。

### 5.5 经验沉淀

1. 长历史扫描（130 天）如果还要复用，优先考虑中间表 / persist。
2. 上游出现、下游未消费、最终未写入结果表的分支，优先删除。
3. `count()` 不是轻量验证，放在主链路里会直接增加一次全量执行。
4. 候选特征远大于最终保留特征时，应该在源码阶段收敛，而不是最后统一 drop。
