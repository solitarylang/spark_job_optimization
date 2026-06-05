# 输入约定

## Case 目录结构

每个分析 case 用一个目录：

```text
input/<case_name>/
  source.zip
  source/
  spark_ui/
  context/
  eventlog/
  notes.md
```

## 源码

- 首选：`source.zip`
- 也可以直接放解压后的 `source/`
- 两者都存在时，把 `source/` 当工作副本，`source.zip` 作为原始证据保留
- 如果有多个源码压缩包，在 `notes.md` 里简短说明哪个是主包
- 为了保留优化前 / 优化后两个版本的代码，不要依赖 git 记录；请直接保留代码快照目录。
- 默认把优化前代码放在 `source/`（或 `source_before/`），优化后代码放在 `source_optimized/`（或 `source_after/`）。
- 如果同一个 case 有多个优化版本，优先保留“优化前 + 最新优化后”两个版本，其他历史版本需要在 `notes.md` 里说明语义。

## Spark UI

把导出的 Spark UI 页面放到 `spark_ui/`：

- `jobs.html`
- `stages.html`
- `sql.html`
- `executors.html`
- `environment.html`

如果是从登录态浏览器里采集的，可把复制出的页面文本放到：

- `spark_ui/browser/environment.txt`
- `spark_ui/browser/jobs.txt`
- `spark_ui/browser/stages.txt`
- `spark_ui/browser/executors.txt`
- `spark_ui/browser/sql.txt`
- `spark_ui/browser/manifest.md`

同一个 case 在分析阶段重复采集时，默认覆盖 `spark_ui/browser/` 这份当前快照，不要每次都新增并行目录。
如果要保留最终优化后的对比版本，建议另存到单独目录，例如 `spark_ui/optimized_browser/`，并在 `notes.md` 里说明它是优化后快照。
分析前的 Spark UI 视为“优化前快照”，优化完成后重新采集的 Spark UI 视为“优化后快照”，两者都要保留，方便做前后对比。

如果导出的文件名不同，就保留原名，并在 `notes.md` 里说明每个文件是什么。

## 上下文

把采集到的集群资源、上游表备注和兜底查询放到 `context/`。

Suggested files:

- `context_report.md`
- `cluster.md`
- `upstream_tables.md`
- `table_queries.md`

## Event Log 和补充证据

- event log 放到 `eventlog/`
- 截图放到 `spark_ui/` 或 `notes/`
- 运行备注、假设、已知限制写到 `notes.md`

## 输出

结果写到：

```text
output/<case_name>/
```

输入和输出要保持同一个 case 名，方便端到端追踪。

## 前后对比约定

- `spark_ui/browser/` 默认表示优化前当前快照。
- `spark_ui/optimized_browser/` 默认表示优化后快照。
- 如果同一个 case 反复优化，保持这两个目录分别代表“优化前 / 优化后”。
- 如果需要保留更早的历史版本，可以另起目录，但必须在 `notes.md` 里说明版本语义。

## 代码版本约定

- `source/` 或 `source_before/` 默认表示优化前代码快照。
- `source_optimized/` 或 `source_after/` 默认表示优化后代码快照。
- 不使用 git 来承载版本对比；需要保留的版本请直接落盘成目录。
- 最终优化前后对比时，必须同时保留这两个代码快照，避免后续变更覆盖掉原始版本。
