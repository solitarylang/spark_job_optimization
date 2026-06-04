# Spark 界面浏览器采集

基础链接：`https://keyhole.ph.seabank.io/cluster/app/application_1772593899018_1041233?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088`

| 页面 | 标题 | 链接 | 文件 |
|---|---|---|---|
| `environment` | `Application application_1772593899018_1041233` | `https://keyhole.ph.seabank.io/cluster/app/application_1772593899018_1041233/environment/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser_from_link/environment.txt` |
| `jobs` | `Application application_1772593899018_1041233` | `https://keyhole.ph.seabank.io/cluster/app/application_1772593899018_1041233/jobs/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser_from_link/jobs.txt` |
| `stages` | `Application application_1772593899018_1041233` | `https://keyhole.ph.seabank.io/cluster/app/application_1772593899018_1041233/stages/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser_from_link/stages.txt` |
| `executors` | `Application application_1772593899018_1041233` | `https://keyhole.ph.seabank.io/cluster/app/application_1772593899018_1041233/executors/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser_from_link/executors.txt` |
| `sql` | `Application application_1772593899018_1041233` | `https://keyhole.ph.seabank.io/cluster/app/application_1772593899018_1041233/sql/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser_from_link/sql.txt` |

## 说明

- 每个文件都保存了浏览器导航后可见的页面文本。
- 这个采集方式依赖当前 Chrome 登录态。
- 将生成的 `spark_ui/browser/` 目录交给 `collect_case_context.py` 继续处理。
