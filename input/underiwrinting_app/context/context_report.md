# 上下文采集报告

Case: `underiwrinting_app`

## 总结

诊断前先整理集群资源形态和上游表上下文。优先使用 Spark 界面 Environment 中已有的数据；缺失指标则输出可直接执行的 SQL / HDFS 兜底命令。

## 已提取的集群资源

| Key | Value |
|---|---|
| `spark.executor.instances` | `18` |
| `spark.executor.cores` | `4` |
| `spark.executor.memory` | `24g` |
| `spark.executor.memoryOverhead` | `2g` |
| `spark.dynamicAllocation.enabled` | `true` |
| `spark.dynamicAllocation.minExecutors` | `1` |
| `spark.dynamicAllocation.maxExecutors` | `100` |
| `spark.sql.shuffle.partitions` | `200` |
| `spark.sql.adaptive.enabled` | `true` |
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` |
| `spark.eventLog.dir` | `hdfs://phlive1/logs/spark/` |

## 缺失资源的查询命令

- `spark.sql.adaptive.coalescePartitions.enabled` -> `grep -R "spark.sql.adaptive.coalescePartitions.enabled" <spark-submit-log-or-dag-config>`
- `spark.sql.adaptive.skewJoin.enabled` -> `grep -R "spark.sql.adaptive.skewJoin.enabled" <spark-submit-log-or-dag-config>`
- `spark.sql.adaptive.advisoryPartitionSizeInBytes` -> `grep -R "spark.sql.adaptive.advisoryPartitionSizeInBytes" <spark-submit-log-or-dag-config>`
- `spark.sql.autoBroadcastJoinThreshold` -> `grep -R "spark.sql.autoBroadcastJoinThreshold" <spark-submit-log-or-dag-config>`
- `spark.default.parallelism` -> `grep -R "spark.default.parallelism" <spark-submit-log-or-dag-config>`
- `spark.sql.files.maxPartitionBytes` -> `grep -R "spark.sql.files.maxPartitionBytes" <spark-submit-log-or-dag-config>`

## 上游表

- `ods.mbs_app_anti_fraud_ss`
- `ods.mbs_dispatch_center_message_log_hi`

## 查询模板

### `ods.mbs_app_anti_fraud_ss`

```sql
DESC FORMATTED ods.mbs_app_anti_fraud_ss;
```
```sql
SHOW PARTITIONS ods.mbs_app_anti_fraud_ss;
```
```sql
SELECT COUNT(1) AS row_cnt FROM ods.mbs_app_anti_fraud_ss WHERE pt_date BETWEEN '<start>' AND '<end>';
```
```sql
SELECT COUNT(1) AS row_cnt FROM ods.mbs_app_anti_fraud_ss;
```
```text
-- After DESC FORMATTED ods.mbs_app_anti_fraud_ss, copy Location and run:
-- hdfs dfs -du -s -h <location>
-- hdfs dfs -count -q <location>
```

### `ods.mbs_dispatch_center_message_log_hi`

```sql
DESC FORMATTED ods.mbs_dispatch_center_message_log_hi;
```
```sql
SHOW PARTITIONS ods.mbs_dispatch_center_message_log_hi;
```
```sql
SELECT COUNT(1) AS row_cnt FROM ods.mbs_dispatch_center_message_log_hi WHERE pt_date BETWEEN '<start>' AND '<end>';
```
```sql
SELECT COUNT(1) AS row_cnt FROM ods.mbs_dispatch_center_message_log_hi;
```
```text
-- After DESC FORMATTED ods.mbs_dispatch_center_message_log_hi, copy Location and run:
-- hdfs dfs -du -s -h <location>
-- hdfs dfs -count -q <location>
```

## 源文件

- `source/high_risk_app.py`

## Spark 界面文件

- `spark_ui/browser/environment.txt`
- `spark_ui/browser/executors.txt`
- `spark_ui/browser/jobs.txt`
- `spark_ui/browser/manifest.json`
- `spark_ui/browser/manifest.md`
- `spark_ui/browser/sql.txt`
- `spark_ui/browser/stages.txt`
- `spark_ui/browser_from_link/environment.txt`
- `spark_ui/browser_from_link/executors.txt`
- `spark_ui/browser_from_link/jobs.txt`
- `spark_ui/browser_from_link/manifest.json`
- `spark_ui/browser_from_link/manifest.md`
- `spark_ui/browser_from_link/sql.txt`
- `spark_ui/browser_from_link/stages.txt`
- `spark_ui/browser_proxy/environment.txt`
- `spark_ui/browser_proxy/executors.txt`
- `spark_ui/browser_proxy/jobs.txt`
- `spark_ui/browser_proxy/manifest.json`
- `spark_ui/browser_proxy/manifest.md`
- `spark_ui/browser_proxy/sql.txt`
- `spark_ui/browser_proxy/stages.txt`

## 原始 JSON

```json
{
  "case_dir": "input/underiwrinting_app",
  "summary": "诊断前先整理集群资源形态和上游表上下文。优先使用 Spark 界面 Environment 中已有的数据；缺失指标则输出可直接执行的 SQL / HDFS 兜底命令。",
  "spark_resources": {
    "spark.executor.instances": "18",
    "spark.executor.cores": "4",
    "spark.executor.memory": "24g",
    "spark.executor.memoryOverhead": "2g",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "100",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.eventLog.dir": "hdfs://phlive1/logs/spark/"
  },
  "upstream_tables": [
    "ods.mbs_app_anti_fraud_ss",
    "ods.mbs_dispatch_center_message_log_hi"
  ],
  "source_files": [
    "source/high_risk_app.py"
  ],
  "spark_ui_files": [
    "spark_ui/browser/environment.txt",
    "spark_ui/browser/executors.txt",
    "spark_ui/browser/jobs.txt",
    "spark_ui/browser/manifest.json",
    "spark_ui/browser/manifest.md",
    "spark_ui/browser/sql.txt",
    "spark_ui/browser/stages.txt",
    "spark_ui/browser_from_link/environment.txt",
    "spark_ui/browser_from_link/executors.txt",
    "spark_ui/browser_from_link/jobs.txt",
    "spark_ui/browser_from_link/manifest.json",
    "spark_ui/browser_from_link/manifest.md",
    "spark_ui/browser_from_link/sql.txt",
    "spark_ui/browser_from_link/stages.txt",
    "spark_ui/browser_proxy/environment.txt",
    "spark_ui/browser_proxy/executors.txt",
    "spark_ui/browser_proxy/jobs.txt",
    "spark_ui/browser_proxy/manifest.json",
    "spark_ui/browser_proxy/manifest.md",
    "spark_ui/browser_proxy/sql.txt",
    "spark_ui/browser_proxy/stages.txt"
  ],
  "missing_cluster_keys": [
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.default.parallelism",
    "spark.sql.files.maxPartitionBytes"
  ],
  "cluster_fallbacks": {
    "spark.sql.adaptive.coalescePartitions.enabled": "grep -R \"spark.sql.adaptive.coalescePartitions.enabled\" <spark-submit-log-or-dag-config>",
    "spark.sql.adaptive.skewJoin.enabled": "grep -R \"spark.sql.adaptive.skewJoin.enabled\" <spark-submit-log-or-dag-config>",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "grep -R \"spark.sql.adaptive.advisoryPartitionSizeInBytes\" <spark-submit-log-or-dag-config>",
    "spark.sql.autoBroadcastJoinThreshold": "grep -R \"spark.sql.autoBroadcastJoinThreshold\" <spark-submit-log-or-dag-config>",
    "spark.default.parallelism": "grep -R \"spark.default.parallelism\" <spark-submit-log-or-dag-config>",
    "spark.sql.files.maxPartitionBytes": "grep -R \"spark.sql.files.maxPartitionBytes\" <spark-submit-log-or-dag-config>"
  },
  "table_queries": {
    "ods.mbs_app_anti_fraud_ss": [
      "DESC FORMATTED ods.mbs_app_anti_fraud_ss;",
      "SHOW PARTITIONS ods.mbs_app_anti_fraud_ss;",
      "SELECT COUNT(1) AS row_cnt FROM ods.mbs_app_anti_fraud_ss WHERE pt_date BETWEEN '<start>' AND '<end>';",
      "SELECT COUNT(1) AS row_cnt FROM ods.mbs_app_anti_fraud_ss;",
      "-- After DESC FORMATTED ods.mbs_app_anti_fraud_ss, copy Location and run:\n-- hdfs dfs -du -s -h <location>\n-- hdfs dfs -count -q <location>"
    ],
    "ods.mbs_dispatch_center_message_log_hi": [
      "DESC FORMATTED ods.mbs_dispatch_center_message_log_hi;",
      "SHOW PARTITIONS ods.mbs_dispatch_center_message_log_hi;",
      "SELECT COUNT(1) AS row_cnt FROM ods.mbs_dispatch_center_message_log_hi WHERE pt_date BETWEEN '<start>' AND '<end>';",
      "SELECT COUNT(1) AS row_cnt FROM ods.mbs_dispatch_center_message_log_hi;",
      "-- After DESC FORMATTED ods.mbs_dispatch_center_message_log_hi, copy Location and run:\n-- hdfs dfs -du -s -h <location>\n-- hdfs dfs -count -q <location>"
    ]
  }
}
```
