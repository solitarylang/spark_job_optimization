# 第 2 步：Spark UI 读取

## 目的

读取执行证据，定位任务耗时在哪里。

## 输入

- `input/<case_name>/spark_ui/`
- `spark_ui/browser/` 页面采集文本
- `input/<case_name>/eventlog/`
- YARN application 链接或 Spark 运行日志链接
- 同一个 case 在分析阶段重复抓取时，默认覆盖 `spark_ui/browser/` 当前快照；最终优化后再抓取的结果另存为单独对比快照

## 采集顺序

1. 如果输入的是 YARN application 链接，先打开 ApplicationMaster 页面，采集 ApplicationMaster 的日志信息。
2. 再从 ApplicationMaster 跳转到 Spark 运行日志页面，继续采集 Spark UI。
3. 如果输入本身就是 Spark 运行日志链接，直接进入 Spark UI 采集。
4. 按顺序查看 `jobs`、`stages`、`executors`、`environment`、`sql` 页面。

## 需要捕捉的内容

- `jobs` 页面：总 job 数、长耗时 job、失败 job、重复提交或重试痕迹
- `stages` 页面：执行时长超过 10 分钟的 stage、每个 stage 的 task 情况、数据倾斜、IO 时长、失败、spill、长尾任务
- `executors` 页面：executor 负载是否均衡、CPU / 内存 / shuffle / GC / spill 使用情况、dead executor
- `environment` 页面：Spark 执行参数、是否开启推测执行、广播阈值、AQE、shuffle 分区数等
- `sql` 页面：SQL 执行计划、join 类型、exchange、window、stage 对应关系
- `eventlog`：可补充 stage / job / task 的原始执行证据

## 输出

- job / stage / executor / environment / SQL 的运行备注
- 瓶颈证据
- 和源码视角一致或冲突的症状
- 每个重要线索的 `已确认` / `待确认` 标记
