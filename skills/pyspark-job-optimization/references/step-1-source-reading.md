# 第 1 步：源码阅读子 skill 调用契约

## 目的

这一节不重复实现源码分析逻辑，只定义如何调用 `subskills/source-reading/SKILL.md`，以及子 skill 交付到最终报告第 1 章的内容边界。

## 由子 skill 完成的工作

- 读取 PySpark 源码
- 按 Spark stage 拆分源码链路
- 建立 `源码 -> stage -> 指标` 对照
- 标出 scan 放大、shuffle 放大、增行、冗余列传递、重复聚合、重复 join、skew、长尾、`count()` / `show()` / `collect()` 等风险点
- 如果同一条业务链在不同日期执行时会重新解析同一批历史数据，要标成重复执行风险点，并说明是否应该改成增量化、按天中间表或结果复用
- 如果某个 stage 对应的读取规模超过 10 亿行或 2T，要明确标成“量级合理性待确认”并补充原因判断，重点检查是否存在重复扫描、字段/分区未裁剪、粒度过粗或需要中间表
- 如果发现冗余中间列，尤其是排序字段、`rank` / `row_number` / `dense_rank` 这类下游没有消费的列，要标成“应删除其计算逻辑”的优化候选，而不是只写末端 drop
- 如果 scan 的是 130 天、360 天这类长历史分区全量表，要明确写出是否可以改成中间表、增量计算或解析结果复用，避免大数据量重复 scan
- 标出上游出现、下游未使用、最终也未写入结果表的冗余字段候选
- 标出每个 stage 的 `已确认` / `待确认`

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
- 每个 stage 下游未消费、最终未写入结果表的冗余字段候选
- 每个 stage 的 `已确认` / `待确认`
- 需要在第 2 步补齐的证据缺口

## 本节不做的事

- 不在这里重新展开完整分析规则
- 不在这里直接下结论替代子 skill
- 不在这里提前写 Spark UI 的运行证据

## 调用要求

调用时只需要把 case 输入和子 skill 一起传入，让子 skill 负责实际分析；本节只负责把分析结果收口成第 1 章素材。
