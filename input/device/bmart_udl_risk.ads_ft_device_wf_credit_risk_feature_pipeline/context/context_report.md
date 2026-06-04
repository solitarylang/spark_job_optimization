# Context Collection Report

Case: `bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline`

## Summary

Collect cluster resource shape and upstream table context before diagnosis. Use Spark UI Environment data when available, and emit fallback SQL / HDFS commands for any missing metrics.

## Found Cluster Resources

| Key | Value |
|---|---|
| `spark.executor.instances` | `5` |
| `spark.executor.cores` | `1` |
| `spark.executor.memory` | `4g` |
| `spark.executor.memoryOverhead` | `2g` |
| `spark.dynamicAllocation.enabled` | `true` |
| `spark.dynamicAllocation.minExecutors` | `1` |
| `spark.dynamicAllocation.maxExecutors` | `100` |
| `spark.sql.shuffle.partitions` | `200` |
| `spark.sql.adaptive.enabled` | `true` |
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` |
| `spark.eventLog.dir` | `hdfs://phlive1/logs/spark/` |

## Missing Cluster Queries

- `spark.sql.adaptive.coalescePartitions.enabled` -> `grep -R "spark.sql.adaptive.coalescePartitions.enabled" <spark-submit-log-or-dag-config>`
- `spark.sql.adaptive.skewJoin.enabled` -> `grep -R "spark.sql.adaptive.skewJoin.enabled" <spark-submit-log-or-dag-config>`
- `spark.sql.adaptive.advisoryPartitionSizeInBytes` -> `grep -R "spark.sql.adaptive.advisoryPartitionSizeInBytes" <spark-submit-log-or-dag-config>`
- `spark.sql.autoBroadcastJoinThreshold` -> `grep -R "spark.sql.autoBroadcastJoinThreshold" <spark-submit-log-or-dag-config>`
- `spark.default.parallelism` -> `grep -R "spark.default.parallelism" <spark-submit-log-or-dag-config>`
- `spark.sql.files.maxPartitionBytes` -> `grep -R "spark.sql.files.maxPartitionBytes" <spark-submit-log-or-dag-config>`

## Upstream Tables

- `bmart_udl_risk.dwd_device_di`
- `fmart_antifraud.dwd_antifraud_action_log_di`

## Query Templates

### `bmart_udl_risk.dwd_device_di`

```sql
DESC FORMATTED bmart_udl_risk.dwd_device_di;
```
```sql
SHOW PARTITIONS bmart_udl_risk.dwd_device_di;
```
```sql
SELECT COUNT(1) AS row_cnt FROM bmart_udl_risk.dwd_device_di WHERE pt_date BETWEEN '<start>' AND '<end>';
```
```sql
SELECT COUNT(1) AS row_cnt FROM bmart_udl_risk.dwd_device_di;
```
```text
-- After DESC FORMATTED bmart_udl_risk.dwd_device_di, copy Location and run:
-- hdfs dfs -du -s -h <location>
-- hdfs dfs -count -q <location>
```

### `fmart_antifraud.dwd_antifraud_action_log_di`

```sql
DESC FORMATTED fmart_antifraud.dwd_antifraud_action_log_di;
```
```sql
SHOW PARTITIONS fmart_antifraud.dwd_antifraud_action_log_di;
```
```sql
SELECT COUNT(1) AS row_cnt FROM fmart_antifraud.dwd_antifraud_action_log_di WHERE pt_date BETWEEN '<start>' AND '<end>';
```
```sql
SELECT COUNT(1) AS row_cnt FROM fmart_antifraud.dwd_antifraud_action_log_di;
```
```text
-- After DESC FORMATTED fmart_antifraud.dwd_antifraud_action_log_di, copy Location and run:
-- hdfs dfs -du -s -h <location>
-- hdfs dfs -count -q <location>
```

## Source Files

- `source/dao/device/client_device.py`
- `source/dao/device/device.py`
- `source/features/device/ft_device.py`
- `source/schedule/pipeline/credit_risk_feature_pipeline.py`

## Spark UI Files

- `spark_ui/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Spark Jobs_files/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Environment.html`
- `spark_ui/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Spark Jobs_files/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Executors.html`
- `spark_ui/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Spark Jobs_files/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - SQL.html`
- `spark_ui/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Spark Jobs_files/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Spark Jobs.html`

## Raw JSON

```json
{
  "case_dir": "input/device/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline",
  "summary": "Collect cluster resource shape and upstream table context before diagnosis. Use Spark UI Environment data when available, and emit fallback SQL / HDFS commands for any missing metrics.",
  "spark_resources": {
    "spark.executor.instances": "5",
    "spark.executor.cores": "1",
    "spark.executor.memory": "4g",
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
    "bmart_udl_risk.dwd_device_di",
    "fmart_antifraud.dwd_antifraud_action_log_di"
  ],
  "source_files": [
    "source/dao/device/client_device.py",
    "source/dao/device/device.py",
    "source/features/device/ft_device.py",
    "source/schedule/pipeline/credit_risk_feature_pipeline.py"
  ],
  "spark_ui_files": [
    "spark_ui/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Spark Jobs_files/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Environment.html",
    "spark_ui/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Spark Jobs_files/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Executors.html",
    "spark_ui/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Spark Jobs_files/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - SQL.html",
    "spark_ui/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Spark Jobs_files/bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline.py - Spark Jobs.html"
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
    "bmart_udl_risk.dwd_device_di": [
      "DESC FORMATTED bmart_udl_risk.dwd_device_di;",
      "SHOW PARTITIONS bmart_udl_risk.dwd_device_di;",
      "SELECT COUNT(1) AS row_cnt FROM bmart_udl_risk.dwd_device_di WHERE pt_date BETWEEN '<start>' AND '<end>';",
      "SELECT COUNT(1) AS row_cnt FROM bmart_udl_risk.dwd_device_di;",
      "-- After DESC FORMATTED bmart_udl_risk.dwd_device_di, copy Location and run:\n-- hdfs dfs -du -s -h <location>\n-- hdfs dfs -count -q <location>"
    ],
    "fmart_antifraud.dwd_antifraud_action_log_di": [
      "DESC FORMATTED fmart_antifraud.dwd_antifraud_action_log_di;",
      "SHOW PARTITIONS fmart_antifraud.dwd_antifraud_action_log_di;",
      "SELECT COUNT(1) AS row_cnt FROM fmart_antifraud.dwd_antifraud_action_log_di WHERE pt_date BETWEEN '<start>' AND '<end>';",
      "SELECT COUNT(1) AS row_cnt FROM fmart_antifraud.dwd_antifraud_action_log_di;",
      "-- After DESC FORMATTED fmart_antifraud.dwd_antifraud_action_log_di, copy Location and run:\n-- hdfs dfs -du -s -h <location>\n-- hdfs dfs -count -q <location>"
    ]
  }
}
```
