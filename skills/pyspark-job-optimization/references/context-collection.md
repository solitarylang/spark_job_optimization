# 上下文采集

## 目的

在开始主诊断前，先收集集群运行上下文和上游表上下文。

## 采集内容

- Spark UI 的 Environment 页面或 `spark-submit` / DAG 配置里的执行参数
- Executor 和集群资源形态
- PySpark 源码里使用到的上游表名
- 表位置、分区形态、行数和存储大小

## 推荐存放位置

把采集结果放到：

```text
input/<case_name>/context/
```

建议文件：

```text
input/<case_name>/context/
  context_report.md
  cluster.md
  upstream_tables.md
  table_queries.md
```

## 工具

优先使用 `scripts/collect_case_context.py`。

如果 Spark UI 只能通过用户登录态 Chrome 访问，就先用 `scripts/collect_spark_ui_browser.py` 把页面内容采集到 `spark_ui/browser/`，尽量保留表格块为 Markdown table，再对 case 目录运行 `collect_case_context.py`。
同一个 case 在分析阶段重复采集时，默认覆盖 `spark_ui/browser/` 这份当前快照；只有最终优化后重新抓取时，才另存到单独目录做前后对比。

如果某个值不能自动采集，脚本要输出可手工查询的命令或 SQL。

## 在线 Spark UI 采集

当 Spark UI 通过代理 URL 提供，并且能在 Chrome 里打开时：

1. 在用户的登录态 Chrome 会话里打开基础链接。
2. 如果入口是 YARN application 链接，先采集 ApplicationMaster 日志页，再进入 Spark 运行日志页。
3. 采集 `jobs`、`stages`、`executors`、`environment` 和 `sql` 页面。
4. 对明显重要或运行超过 30 分钟的 job / stage，补采对应详情页和 task 级信息。
5. 保存每页可见内容，优先保留表格块为 Markdown table，以及包含最终 URL 和标题的小清单。
6. 再把采集文件喂给 `collect_case_context.py`。

采集到的页面内容要当作证据，不要当作摘要。原始页面内容要留在 case 目录里，并尽量保留表格块和详情页。

## 兜底查询

### Spark / 集群

如果 Spark UI / Environment 数据缺失，就查询或记录：

- executor count
- executor cores
- executor memory
- dynamic allocation
- `spark.sql.shuffle.partitions`
- `spark.sql.adaptive.enabled`
- `spark.sql.autoBroadcastJoinThreshold`

### 上游表

每张上游表都要收集：

1. `DESC FORMATTED <table>;`
2. `SHOW PARTITIONS <table>;`
3. `SELECT COUNT(1) AS row_cnt FROM <table> WHERE pt_date BETWEEN '<start>' AND '<end>';`
4. `SELECT COUNT(1) AS row_cnt FROM <table>;`
5. After `DESC FORMATTED`, use the table `Location` with:
   - `hdfs dfs -du -s -h <location>`
   - `hdfs dfs -count -q <location>`

如果分区列不是 `pt_date`，就替换成真实分区字段。

## 输出规则

如果指标采不到，不要猜。直接输出需要执行的命令或 SQL。
