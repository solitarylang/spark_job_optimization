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
- `withColumn` / `select` / `alias` / `drop` 这类列级变换
- `explode` / `explode_outer` / `flatMap` / `posexplode` 这类增行点
- `count()` / `show()` / `collect()` / `take()` / `toPandas()` 这类 action 点
- 冗余字段、冗余分区、冗余中间列
- 可能引发 skew、长尾、窗口排序的大分区操作

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
- `count()` / `show()` / `collect()` / `take()` / `toPandas()` 行为是否会触发全量执行
- 需要第 2 步补齐的运行证据

## 约束

- 不直接做根因排序。
- 不直接输出优化建议。
- 不把 Spark UI 的结论提前写成源码结论。
- 每个算子都要尽量能回指到一个 stage。
- 源码分析必须按 Spark stage 拆分，而不是只按文件或函数拆分。
- 看到一个算子时，必须能回指到它所在的 stage、该 stage 的运行时长、任务数、输入输出和 shuffle 指标；如果不能回指，就先标 `待确认`。
