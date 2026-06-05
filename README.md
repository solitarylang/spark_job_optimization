# spark_job_optimization

这是一个用于沉淀和复用 **PySpark 作业性能分析与优化** 流程的仓库。

核心目标不是只给出一句优化建议，而是把优化前分析、优化方案、优化后验证和经验沉淀都落成可复用产物。

## 仓库结构

- `skills/pyspark-job-optimization/`：主 skill
- `skills/pyspark-job-optimization/subskills/`：可独立执行的子 skill
  - `source-reading`：源码分析
  - `spark-ui-reading`：Spark UI / 日志分析
- `skills/pyspark-job-optimization/references/`：规范、模板、诊断规则、输入约定
- `skills/pyspark-job-optimization/scripts/`：采集、渲染、汇总脚本
- `input/<case>/`：每个分析 case 的输入
- `output/<case>/`：每个 case 的最终报告、图片和对比结果

## 标准流程

1. 采集上下文
   - Spark UI / YARN / 环境 / 上游表 / 运行命令等信息
2. 调用 `source-reading` 子 skill
   - 输出源码执行路径、逻辑、stage 对照和潜在风险
3. 调用 `spark-ui-reading` 子 skill
   - 输出 jobs / stages / executors / SQL / 环境证据
4. 主 skill 做根因分析与优化方案
   - 输出第 3、4 章内容
   - 生成第 4 步优化图片
5. 执行确认后的代码变更
   - 只保留优化前 / 优化后两个代码快照
6. 采集优化后日志并做对比
   - 输出优化效果和经验沉淀

## 输出约定

每个 case 的最终产物至少包含：

- `final_optimization_report.md`
- `step4_top5_bottlenecks.md`
- `step4_top5_bottlenecks.svg`
- `step4_top5_bottlenecks.png`

第 4 步图片的要求是：

- 左侧展示完整源代码
- 用虚线框按 Spark stage 包住对应代码块
- 右侧展示热点放大镜卡片
- 卡片里必须包含当前执行情况、瓶颈原因、预期收益、预期修改代码或草图
- 不能出现文字重叠、压线或越框

## 输入约定

- 同一个 case 的 Spark UI 采集默认覆盖同目录快照
- 优化前后对比时，分别保留当前版本和优化后版本
- `input/tmp*/`、`output/tmp*/` 这类临时目录不纳入版本管理

## 使用方式

直接在仓库里维护 case 输入、运行脚本生成输出，然后把最终结果放到 `output/<case>/`。

如果是新的 Spark 任务分析，优先补齐：

1. `input/<case>/source/`
2. `input/<case>/spark_ui/browser/`
3. `input/<case>/context/context_report.md`
4. `output/<case>/final_optimization_report.md`
5. `output/<case>/step4_top5_bottlenecks.svg`
6. `output/<case>/step4_top5_bottlenecks.png`

