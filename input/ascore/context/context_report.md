# ascore 上下文报告

## 1. 案例信息

- Case：`ascore`
- Spark UI application：`application_1772593899018_1055219`
- Spark UI 基础链接：`https://keyhole.ph.seabank.io/history/application_1772593899018_1055219/1/?originSchema=http&originHost=10.163.90.104%3A18088`

## 2. 源码入口

- `source/loanstatus_offline_prod_hdfs.py`
- `source/util_helper/spark_helper.py`
- `source/util_helper/woe_helper_hdfs.py`

## 3. 已确认的集群资源

| Key | Value |
|---|---|
| `spark.executor.instances` | `32` |
| `spark.executor.cores` | `4` |
| `spark.executor.memory` | `16g` |
| `spark.executor.memoryOverhead` | `2g` |
| `spark.dynamicAllocation.enabled` | `False` |
| `spark.dynamicAllocation.minExecutors` | `1` |
| `spark.dynamicAllocation.maxExecutors` | `32` |
| `spark.sql.shuffle.partitions` | `200` |
| `spark.sql.adaptive.enabled` | `true` |
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` |
| `spark.eventLog.dir` | `hdfs://phlive1/logs/spark/` |

## 4. 上游表

- `bmart_udl_risk.channel_loan_credit_risk_tmp`
- `fmart_loan.dwd_loan_application_dc`
- `dws.t80_dim_time_cs_d`
- `fmart_loan.dwd_loan_accounting_df`

## 5. 兜底查询

### 5.1 Spark / 集群

- `grep -R "spark.sql.adaptive.coalescePartitions.enabled" <spark-submit-log-or-dag-config>`
- `grep -R "spark.sql.adaptive.skewJoin.enabled" <spark-submit-log-or-dag-config>`
- `grep -R "spark.sql.adaptive.advisoryPartitionSizeInBytes" <spark-submit-log-or-dag-config>`
- `grep -R "spark.sql.autoBroadcastJoinThreshold" <spark-submit-log-or-dag-config>`
- `grep -R "spark.default.parallelism" <spark-submit-log-or-dag-config>`
- `grep -R "spark.sql.files.maxPartitionBytes" <spark-submit-log-or-dag-config>`

### 5.2 上游表

```sql
DESC FORMATTED <table>;
SHOW PARTITIONS <table>;
SELECT COUNT(1) AS row_cnt FROM <table> WHERE pt_date BETWEEN '<start>' AND '<end>';
SELECT COUNT(1) AS row_cnt FROM <table>;
```

```text
-- After DESC FORMATTED <table>, copy Location and run:
-- hdfs dfs -du -s -h <location>
-- hdfs dfs -count -q <location>
```

## 6. Spark UI 浏览器采集文件

- `spark_ui/browser/environment.txt`
- `spark_ui/browser/jobs.txt`
- `spark_ui/browser/stages.txt`
- `spark_ui/browser/executors.txt`
- `spark_ui/browser/sql.txt`
- `spark_ui/browser/manifest.md`
- `spark_ui/browser/manifest.json`
