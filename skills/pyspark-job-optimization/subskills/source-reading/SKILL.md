---
name: pyspark-source-reading
description: 独立执行 PySpark 源码分析，按 Spark stage 拆分代码路径并输出第 1 章素材。适用于只需要源码侧执行链路、风险点和 stage 对齐分析的场景。
---

# PySpark 源码分析子 skill

## 目的

只做源码阅读，不进入运行日志归因。目标是把代码路径按 Spark stage 拆开，形成 `源码 -> stage -> 指标` 对照。

## 输入

- `input/<case_name>/source.zip` 或 `input/<case_name>/source/`
- `input/<case_name>/context/context_report.md`（如有）
- `input/<case_name>/spark_ui/browser/` 或 `eventlog/` 中可用于 stage 对齐的证据

## 工作方式

1. 先读入口和主链路，找出所有 DataFrame / RDD / action 的落点。
2. 把代码链路按 Spark stage 切分。
3. 对每个 stage 记录对应的算子链、输入输出、潜在增行点、shuffle 点、冗余列和重复聚合。
4. 如果某段源码还不能明确映射到 stage，就标成 `待确认`。

## 需要重点识别的源码信号

- 入口 / 出口点
- 主数据流和分支数据流
- 每个 action 的触发位置
- 重复读取、重复 join、重复聚合、重复扫描
- 跨日期重复解析同一批历史数据的逻辑，要优先标记为重复执行候选，检查是否应该改成增量化、按天中间表或解析结果复用
- `withColumn` / `select` / `alias` / `drop` 这类列级变换
- 如果发现冗余中间列，尤其是排序字段、`rank` / `row_number` / `dense_rank` 这类下游没有消费的列，要优先删除其计算逻辑，而不是只在结果阶段 drop
- `explode` / `explode_outer` / `flatMap` / `posexplode` 这类增行点
- `count()` / `show()` / `collect()` / `take()` / `toPandas()` 这类 action 点
- 如果源码映射出来的某个 stage 预计或已知会读取超过 10 亿行，或者输入规模超过 2T，要先标成量级合理性待确认，重点检查是否存在重复扫描、字段/分区未裁剪、粒度过粗或需要中间表
- 如果同一条业务链在不同日期都会重新解析历史数据，要标成重复执行候选，优先检查是否可以前置解析并复用结果
- 冗余字段、冗余分区、冗余中间列
- 上游出现、下游未使用、最终也未写入结果表的字段，优先标记为冗余字段并评估是否可删除
- 上游出现但下游不消费的排序字段和中间列，要优先直接删除其计算逻辑，而不是只在结果阶段清理
- 可能引发 skew、长尾、窗口排序的大分区操作
- 遇到窗口排序、groupBy、join 这类 shuffle 慢点时，要先判断是总量问题还是倾斜问题，再决定后续是否落表或重构。

## 输出

- `源码 -> stage -> 指标` 对照表
- 按 stage 拆分的源码分析结果
- 每个 stage 对应的代码范围、算子链路、数据流和 action 点
- 每个 stage 的风险点与观察项
- 每个 stage 的 `已确认` / `待确认` 标记
- 需要在运行日志分析阶段补齐的证据缺口

## 必须产出的内容

- Stage 级源码对照表
- 关键算子链路说明
- 增行 / shuffle / skew / 冗余列 / 冗余分区风险点
- 冗余字段是否在下游未消费且最终未写入结果表；如果是，要明确标记为可删候选
- `count()` / `show()` / `collect()` / `take()` / `toPandas()` 行为是否会触发全量执行
- 需要第 2 步补齐的运行证据

## 约束

- 不直接做根因排序。
- 不直接输出优化建议。
- 不把 Spark UI 的结论提前写成源码结论。
- 每个算子都要尽量能回指到一个 stage。
- 源码分析必须按 Spark stage 拆分，而不是只按文件或函数拆分。
- 看到一个算子时，必须能回指到它所在的 stage、该 stage 的运行时长、任务数、输入输出和 shuffle 指标；如果不能回指，就先标 `待确认`。
