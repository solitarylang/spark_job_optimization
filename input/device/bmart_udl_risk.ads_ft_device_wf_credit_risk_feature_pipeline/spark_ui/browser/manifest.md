# Spark UI Browser Collection

Base URL: `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1043091/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088`

| Page | Title | URL | File |
|---|---|---|---|
| `environment` | `S3DefaultJobName - Environment` | `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1043091/environment/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser/environment.txt` |
| `jobs` | `S3DefaultJobName - Spark Jobs` | `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1043091/jobs/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser/jobs.txt` |
| `stages` | `S3DefaultJobName - Stages for All Jobs` | `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1043091/stages/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser/stages.txt` |
| `executors` | `S3DefaultJobName - Executors` | `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1043091/executors/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser/executors.txt` |
| `sql` | `S3DefaultJobName - Spark Jobs` | `https://keyhole.ph.seabank.io/proxy/application_1772593899018_1043091/sql/?originHost=hadoop-ph-live09-10-163-90-104%3A8088&originIp=hadoop-ph-live09-10-163-90-104&originPort=8088` | `browser/sql.txt` |

## Notes

- Each file contains the visible text copied from the page after browser navigation.
- This collection method works with the current Chrome login session.
- Feed the resulting `spark_ui/browser/` directory into `collect_case_context.py`.
