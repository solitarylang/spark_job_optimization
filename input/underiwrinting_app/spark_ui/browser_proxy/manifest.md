# Spark 界面浏览器采集

基础链接：`https://keyhole.ph.seabank.io/proxy/application_1772593899018_1041233/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088`

| 页面 | 标题 | 链接 | 文件 |
|---|---|---|---|
| `environment` | `ads_udl_customer_underwriting_feature_app_df - Environment` | `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1041233/environment/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser_proxy/environment.txt` |
| `jobs` | `ads_udl_customer_underwriting_feature_app_df - Spark Jobs` | `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1041233/jobs/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser_proxy/jobs.txt` |
| `stages` | `ads_udl_customer_underwriting_feature_app_df - Stages for All Jobs` | `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1041233/stages/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser_proxy/stages.txt` |
| `executors` | `ads_udl_customer_underwriting_feature_app_df - Executors` | `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1041233/executors/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser_proxy/executors.txt` |
| `sql` | `ads_udl_customer_underwriting_feature_app_df - Spark Jobs` | `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1041233/sql/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser_proxy/sql.txt` |

## 说明

- 每个文件都保存了浏览器导航后可见的页面文本。
- 这个采集方式依赖当前 Chrome 登录态。
- 将生成的 `spark_ui/browser/` 目录交给 `collect_case_context.py` 继续处理。
