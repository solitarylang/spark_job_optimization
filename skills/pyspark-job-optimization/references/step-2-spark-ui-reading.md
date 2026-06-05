# 第 2 步：Spark UI / 日志阅读子 skill 调用契约

## 目的

这一节不重复实现 Spark UI 分析逻辑，只定义如何调用 `subskills/spark-ui-reading/SKILL.md`，以及子 skill 交付到最终报告第 2 章的内容边界。

## 由子 skill 完成的工作

- 直接调用 `subskills/spark-ui-reading/SKILL.md`，由子 skill 负责实际的 Spark UI / 日志分析
- 由子 skill 采集并整理 `jobs`、`stages`、`executors`、`environment`、`sql`、`eventlog`、`AM`、`driver`、`executor`、`YARN diagnostics` 等证据
- 由子 skill 在可直接确认时顺手捕获 SQL 原文、执行计划和对应 stage 关系
- 由子 skill 输出可直接写入最终报告第 2 章的素材，包括运行证据、标签判定和优化前后对比项

## 输入

- `input/<case_name>/spark_ui/`
- `spark_ui/browser/` 页面采集文本
- `spark_ui/optimized_browser/` 页面采集文本（如有）
- `input/<case_name>/eventlog/`
- YARN application 链接或 Spark 运行日志链接
- `subskills/spark-ui-reading/SKILL.md`

## 子 skill 输出

子 skill 的输出必须能直接写入最终报告第 2 章，至少包括：

- 集群与环境信息
- Jobs / Stages / Executors / SQL / EventLog / AM / Driver 证据
- 可直接定位到的 SQL 原文、执行计划、对应 stage 关系
- 关键 stage 的耗时、任务数、shuffle、失败、spill
- 关键 stage 的 file listing、路径数、文件数或平均文件大小相关证据；如果能直接看出小文件特征，要一并写明
- 运行时的 `已确认` / `待确认`
- 如果存在优化后快照，还要保留前后对比项

## 本节不做的事

- 不在这里重新展开完整采集细则
- 不在这里把推断写成已确认结论
- 不在这里替代子 skill 直接下最终根因
- 不在这里重复实现子 skill 已有的采集和判定规则

## 调用要求

调用时只需要把 case 输入和子 skill 一起传入，让子 skill 负责实际采集和整理；本节只负责把结果收口成第 2 章素材。
