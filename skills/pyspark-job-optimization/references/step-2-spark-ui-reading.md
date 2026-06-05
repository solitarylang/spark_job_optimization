# 第 2 步：Spark UI / 日志阅读子 skill 调用契约

## 目的

这一节不重复实现 Spark UI 分析逻辑，只定义如何调用 `subskills/spark-ui-reading/SKILL.md`，以及子 skill 交付到最终报告第 2 章的内容边界。

## 由子 skill 完成的工作

- 读取 Spark UI / YARN / AM / driver / executor / eventlog 证据
- 按顺序采集 `jobs`、`stages`、`executors`、`environment`、`sql`
- 采集 `ApplicationMaster`、`driver`、`executor`、`YARN diagnostics`、failure reason 等补充日志
- 标出执行时长、任务数、shuffle、失败、spill、preempt、dead executor、长尾、skew 等运行证据
- 统计并标记超大 stage 数量；如果读取超过 10 亿行 / 2T 或运行超过 30 分钟的 stage 超过 4 个，要额外标出，供后续判断是否需要把部分 stage 落表
- 如果某个 stage 的读取行数超过 10 亿，或者输入规模超过 2T，要额外标记为量级异常候选，并检查是否合理、是否存在重复扫描、字段/分区裁剪不足或中间表缺失
- 对于运行时间超过 30 分钟的 job 或 stage，继续点击对应 stage，采集 task 级别的更细运行信息，包括 task duration 分布、失败 task、shuffle、spill、长尾和是否存在 skew
- 按 `spark_ui_label_definition.sql` 的同级口径，顺手判定失败、重试、长时、排队、内存不足 / 浪费、skew、full GC、spill、任务健康等标签
- 如果存在 `spark_ui/optimized_browser/`，再补优化前后对比

## 输入

- `input/<case_name>/spark_ui/`
- `spark_ui/browser/` 页面采集文本
- `spark_ui/optimized_browser/` 页面采集文本（如有）
- `input/<case_name>/eventlog/`
- YARN application 链接或 Spark 运行日志链接
- `subskills/spark-ui-reading/SKILL.md`

## 采集顺序

1. 如果输入是 YARN application 链接，先走 ApplicationMaster，再进入 Spark 运行日志页面。
2. 如果输入已经是 Spark 运行日志链接，直接采集。
3. 按顺序看 `jobs`、`stages`、`executors`、`environment`、`sql`。
4. 需要时再补 `eventlog` 和其他原始日志。
5. 如果某个环节读取行数超过 10 亿，或者输入规模超过 2T，先检查量级是否合理，再判断慢因。

## 子 skill 输出

子 skill 的输出必须能直接写入最终报告第 2 章，至少包括：

- 集群与环境信息
- Jobs / Stages / Executors / SQL / EventLog / AM / Driver 证据
- 关键 stage 的耗时、任务数、shuffle、失败、spill
- 运行时的 `已确认` / `待确认`
- 如果存在优化后快照，还要保留前后对比项

## 本节不做的事

- 不在这里重新展开完整采集细则
- 不在这里把推断写成已确认结论
- 不在这里替代子 skill 直接下最终根因

## 调用要求

调用时只需要把 case 输入和子 skill 一起传入，让子 skill 负责实际采集和整理；本节只负责把结果收口成第 2 章素材。
