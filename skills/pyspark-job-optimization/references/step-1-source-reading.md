# 第 1 步：源码阅读子 skill 调用契约

## 目的

这一节不重复实现源码分析逻辑，只定义如何调用 `subskills/source-reading/SKILL.md`，以及子 skill 交付到最终报告第 1 章的内容边界。

## 由子 skill 完成的工作

- 直接调用 `subskills/source-reading/SKILL.md`，由子 skill 负责实际源码分析
- 由子 skill 输出可直接写入最终报告第 1 章的素材，包含 stage 级源码对照、风险点、观察项和证据缺口
- 由子 skill 在能直接确认时顺手标出冗余字段、小文件、重复执行、量级异常等候选项

## 输入

- `input/<case_name>/source.zip` 或 `input/<case_name>/source/`
- 主入口文件、核心 transformation、helper module
- `input/<case_name>/context/` 里的上下游表和集群上下文（如有）
- `subskills/source-reading/SKILL.md`

## 子 skill 输出

子 skill 的输出必须能直接写入最终报告第 1 章，至少包括：

- 按 Spark stage 划分的源码分析结果
- `源码 -> stage -> 指标` 对照表
- 每个 stage 对应的代码范围、算子链路和数据流说明
- 每个 stage 的风险点和观察项
- 每个 stage 的 `已确认` / `待确认`
- 需要在第 2 步补齐的证据缺口

## 本节不做的事

- 不在这里重新展开完整分析规则
- 不在这里直接下结论替代子 skill
- 不在这里提前写 Spark UI 的运行证据
- 不在这里重复子 skill 已有的 stage / 风险判定规则

## 调用要求

调用时只需要把 case 输入和子 skill 一起传入，让子 skill 负责实际分析；本节只负责把分析结果收口成第 1 章素材。
