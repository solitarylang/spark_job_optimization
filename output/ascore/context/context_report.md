# ascore 上下文报告

## 1. 案例信息

- Case 名称：`ascore`
- Spark UI application：
  - `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1055219/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088`
- 源码入口：
  - `input/ascore/source/loanstatus_offline_prod_hdfs.py`

## 2. 输入内容

- `input/ascore/source/loanstatus_offline_prod_hdfs.py`
- `input/ascore/source/util_helper/spark_helper.py`
- `input/ascore/source/util_helper/woe_helper_hdfs.py`
- `input/ascore/source/CREDIT_RISK_v2_ascore_pkgs.zip`
- `input/ascore/note.md`

## 3. 当前可直接确认的上下文

- 任务是一个 PySpark 信用风险评分任务，主流程为：
  1. 拉取贷款 / 逾期 / 放款 / 还款事实
  2. 构造特征
  3. 做 WOE 转换
  4. 做模型预测
  5. 写入 Hive 结果表
- 源码里存在明显的 130 天历史扫描、多个 `groupBy` / `pivot` / `join`、以及最终 `count()` 再写表。
- `spark_ui/` 目录当前为空，未见已导出的页面文本或 event log。

## 4. 当前受限项

- Spark UI 页面访问返回 `401 Authorization Required`
- 当前没有可直接读取的 jobs / stages / executors / SQL / eventlog 文本
- 因此运行时长、shuffle、spill、失败、skew、executor 状态均为 `待确认`

## 5. 需要后续补采的运行证据

- `Application Overview`
- `Environment`
- `Jobs`
- `Stages`
- `Executors`
- `SQL`
- `AM / driver / executor / YARN diagnostics`

