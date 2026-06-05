# Spark 界面浏览器采集

基础链接：`https://keyhole.ph.seabank.io/history/application_1772593899018_1055219/1/?originSchema=http&originHost=10.163.90.104%3A18088`

| 页面 | 标题 | 链接 | 文件 |
|---|---|---|---|
| `environment` | `zx_prod_loanstatus - Environment` | `https://keyhole.ph.seabank.io/history/application_1772593899018_1055219/1/environment/?originSchema=http&originHost=10.163.90.104%3A18088` | `browser/environment.txt` |
| `jobs` | `zx_prod_loanstatus - Spark Jobs` | `https://keyhole.ph.seabank.io/history/application_1772593899018_1055219/1/jobs/?originSchema=http&originHost=10.163.90.104%3A18088` | `browser/jobs.txt` |
| `stages` | `zx_prod_loanstatus - Stages for All Jobs` | `https://keyhole.ph.seabank.io/history/application_1772593899018_1055219/1/stages/?originSchema=http&originHost=10.163.90.104%3A18088` | `browser/stages.txt` |
| `executors` | `zx_prod_loanstatus - Executors` | `https://keyhole.ph.seabank.io/history/application_1772593899018_1055219/1/executors/?originSchema=http&originHost=10.163.90.104%3A18088` | `browser/executors.txt` |
| `sql` | `zx_prod_loanstatus - Spark Jobs` | `https://keyhole.ph.seabank.io/history/application_1772593899018_1055219/1/jobs/?originSchema=http&originHost=10.163.90.104%3A18088` | `browser/sql.txt` |

## 说明

- 每个文件都保存了浏览器导航后可见的页面文本。
- 这个采集方式依赖当前 Chrome 登录态。
- 将生成的 `spark_ui/browser/` 目录交给 `collect_case_context.py` 继续处理。
