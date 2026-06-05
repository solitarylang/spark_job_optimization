# 最终优化报告模板

这是整份最终优化报告的总模板。四个章节分别对应流程中的第 1 到第 4 步，后续所有案例都应尽量贴近这个结构。

## 目录结构

```text
output/<case_name>/
  final_optimization_report.md
  final_optimization_report.svg
  final_optimization_report.png
  step4_top5_bottlenecks.md
  step4_top5_bottlenecks.svg
  step4_top5_bottlenecks.png
  context/
    context_report.md
  spark_ui/
    browser/
    ...
```

## 页面结构

```text
# 最终优化报告

案例：`<case_name>`

## 第一章：源码分析
### 1.1 输入与执行路径
### 1.2 主要逻辑与数据流
### 1.3 潜在风险与增行点

## 第二章：运行日志分析
### 2.1 集群与环境信息
### 2.2 Jobs / Stages / Executors
### 2.3 SQL / EventLog / AM / Driver 证据

## 第三章：根因分析
### 3.1 证据对齐
### 3.2 前五个主要瓶颈
### 3.3 已确认 / 待确认

## 第四章：优化方案
### 4.1 优化优先级
### 4.2 参数调优候选
### 4.3 实现方式优化
### 4.4 业务逻辑优化
### 4.5 待确认项
```

## 每章必须产出

### 第一章：源码分析

- `源码 -> stage -> 指标` 对照表
- 按 stage 划分的代码路径说明
- 每个 stage 的算子链路
- 每个 stage 的风险点和观察项
- 主要由源码分析子 skill 产出

### 第二章：运行日志分析

- 集群 / 环境信息
- Jobs / Stages / Executors / SQL / EventLog / AM / Driver 证据
- 每个关键 stage 的任务数、耗时、shuffle、失败、spill 信息
- 主要由 Spark UI / 日志分析子 skill 产出

### 第三章：根因分析

- 前五个根因
- 每个根因对应的 stage 和代码范围
- 每个根因的证据、影响度和待确认项

### 第四章：优化方案

- 参数调优候选
- 实现方式优化
- 业务逻辑优化
- 每项优化的方向、风险和待确认项

## 填写要求

### 第一章：源码分析

- 只写源码里直接可见的执行路径、逻辑关系和潜在风险。
- 必须说明哪些位置会造成 scan 放大、shuffle 放大、增行、冗余列传递或重复聚合。
- 不要把 Spark UI 结论提前写进这一章。

### 第二章：运行日志分析

- 只写 Spark UI、Spark 日志、YARN / AM / driver / executor 日志里直接可见的信息。
- 必须把环境、jobs、stages、executors、SQL 和失败证据分开写清楚。
- 如果某个值没有直接暴露，就写 `待确认` 或 `未直接给出`，不要补推断。

### 第三章：根因分析

- 只能基于前两章的事实加上已有经验规则做归因。
- 前五个问题按影响度排序，但不能把没有证据支撑的内容放进主排名。
- 如果只是观察到但还不能确认，就进入观察项，不进入主瓶颈。

### 第四章：优化方案

- 必须按 `Spark 参数 -> 实现方式 -> 业务逻辑` 的顺序输出。
- 没有明确可落地代码时，也必须给出优化方向。
- 只有源码和日志都能清楚验证时，才给预期修改代码。
- 这一章的详细结构应继续参考 `references/step-4-report-template.md`。

## 图片要求

- 最终报告可以配一张总览图，但第 4 章的单独图片仍然保留。
- 图片里的说明文字必须是中文，代码和命令保留原样。
- 不允许把推断性文字写成已确认结论。
