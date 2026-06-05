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

## 可额外采集的信息

如果只看 Spark UI 还不能解释“为什么慢”或“为什么失败”，再补下面这些原始信息：

- `ApplicationMaster` 日志：AM 是否启动成功、是否被杀、是否拿不到资源、是否有提交失败或重试
- `driver` 日志：Python / JVM 异常栈、DataFrame 写入失败、task 组装失败、最终抛错位置
- `executor` 日志：`OutOfMemoryError`、`GC overhead`、`Container killed`、`FetchFailed`、`FileNotFound`、`DiskError`
- `YARN diagnostics`：`preempted`、`killed by user`、`exit code`、`lost container`、`node lost`
- `stage / task failure reason`：失败任务是否集中在某个 stage、某个 partition、某台机器
- `executor loss reason`：executor 是 OOM、抢占、节点失联，还是本地磁盘异常
- `blacklist` / `exclude` 结果：是否有节点或 executor 被反复拉黑，导致可用资源变少
- `GC / spill / memory` 相关指标：是否存在频繁 Full GC、spill 过大、内存抖动
- `shuffle / fetch` 相关日志：shuffle 文件丢失、shuffle fetch 失败、metadata 不一致
- `提交命令`：`spark-submit` / Airflow / DAG 的实际参数，确认是否和 UI 一致

## 输出

- job / stage / executor / environment / SQL 的运行备注
- 瓶颈证据
- 和源码视角一致或冲突的症状
- 每个重要线索的 `已确认` / `待确认` 标记
