with job_base as (
    select
        label_indicator.pt_date,
        label_indicator.job_id,
        label_indicator.user_id,
        label_indicator.user_name,
        label_indicator.team_id,
        label_indicator.team_name,
        label_indicator.app_name,
        label_indicator.task_table_name,
        label_indicator.app_queue,
        label_indicator.app_type,
        label_indicator.final_state,
        label_indicator.attempt_number,
        label_indicator.airflow_try_number,
        label_indicator.avg_memory_allocate_gb,
        label_indicator.input_size_gb,
        label_indicator.gb_max_shuffle_read_size,
        label_indicator.gb_avg_shuffle_read_size,
        label_indicator.gb_max_shuffle_write_size,
        label_indicator.gb_avg_shuffle_write_size,
        label_indicator.major_gc_time,
        label_indicator.task_time,
        label_indicator.spill_disk_gb,
        label_indicator.elapsed_time_min,
        label_indicator.queue_wait_time,
        label_indicator.memory_gb_hour,
        label_indicator.started_date,
        row_number() over (
            partition by label_indicator.pt_date, label_indicator.job_id
            order by label_indicator.started_date desc
        ) as rn
    from fmart_dw.ads_meta_yarn_task_label_indicator_di label_indicator
    where label_indicator.pt_date >= date_add('2026-06-01', -6)
      and label_indicator.pt_date <= '2026-06-01'
      and date(started_date) = date(pt_date)
),
job_dedup as (
    select *
    from job_base
    where rn = 1
),
job_rank as (
    select
        job_dedup.*,
        case
            when lower(coalesce(job_dedup.app_queue, '')) like '%p0%'
                 or lower(coalesce(job_dedup.app_queue, '')) like '%p1%' then 1
            else 0
        end as is_p0_p1_task,
        row_number() over (partition by job_dedup.pt_date order by coalesce(job_dedup.memory_gb_hour, 0) desc) as mu_rank,
        row_number() over (partition by job_dedup.pt_date order by coalesce(job_dedup.elapsed_time_min, 0) desc) as runtime_rank,
        row_number() over (
            partition by job_dedup.pt_date,
                         case
                             when lower(coalesce(job_dedup.app_queue, '')) like '%p0%'
                                  or lower(coalesce(job_dedup.app_queue, '')) like '%p1%' then 1
                             else 0
                         end
            order by coalesce(job_dedup.elapsed_time_min, 0) desc
        ) as runtime_rank_in_group
    from job_dedup
),
job_daily_top_label as (
    select
        job_rank.*,
        case when mu_rank <= 100 then 1 else 0 end as mu_top100_daily_label,
        case when mu_rank <= 50 then 1 else 0 end as mu_top50_daily_label,
        case when runtime_rank <= 100 then 1 else 0 end as runtime_top100_daily_label,
        case when runtime_rank <= 50 then 1 else 0 end as runtime_top50_daily_label,
        case when is_p0_p1_task = 1 and runtime_rank_in_group <= 100 then 1 else 0 end as p0_p1_runtime_top100_daily_label,
        case when is_p0_p1_task = 1 and runtime_rank_in_group <= 50 then 1 else 0 end as p0_p1_runtime_top50_daily_label,
        case when is_p0_p1_task = 0 and runtime_rank_in_group <= 100 then 1 else 0 end as non_p0_p1_runtime_top100_daily_label,
        case when is_p0_p1_task = 0 and runtime_rank_in_group <= 50 then 1 else 0 end as non_p0_p1_runtime_top50_daily_label
    from job_rank
),
task_day_top_label as (
    select
        task_table_name,
        pt_date,
        max(mu_top100_daily_label) as mu_top100_daily_label,
        max(mu_top50_daily_label) as mu_top50_daily_label,
        max(runtime_top100_daily_label) as runtime_top100_daily_label,
        max(runtime_top50_daily_label) as runtime_top50_daily_label,
        max(p0_p1_runtime_top100_daily_label) as p0_p1_runtime_top100_daily_label,
        max(p0_p1_runtime_top50_daily_label) as p0_p1_runtime_top50_daily_label,
        max(non_p0_p1_runtime_top100_daily_label) as non_p0_p1_runtime_top100_daily_label,
        max(non_p0_p1_runtime_top50_daily_label) as non_p0_p1_runtime_top50_daily_label
    from job_daily_top_label
    group by task_table_name, pt_date
),
task_7d_top_streak as (
    select
        task_table_name,
        case when count(distinct pt_date) = 7 and sum(mu_top100_daily_label) = 7 then 1 else 0 end as mu_top100_label,
        case when count(distinct pt_date) = 7 and sum(mu_top50_daily_label) = 7 then 1 else 0 end as mu_top50_label,
        case when count(distinct pt_date) = 7 and sum(runtime_top100_daily_label) = 7 then 1 else 0 end as runtime_top100_label,
        case when count(distinct pt_date) = 7 and sum(runtime_top50_daily_label) = 7 then 1 else 0 end as runtime_top50_label,
        case when count(distinct pt_date) = 7 and sum(p0_p1_runtime_top100_daily_label) = 7 then 1 else 0 end as p0_p1_runtime_top100_label,
        case when count(distinct pt_date) = 7 and sum(p0_p1_runtime_top50_daily_label) = 7 then 1 else 0 end as p0_p1_runtime_top50_label,
        case when count(distinct pt_date) = 7 and sum(non_p0_p1_runtime_top100_daily_label) = 7 then 1 else 0 end as non_p0_p1_runtime_top100_label,
        case when count(distinct pt_date) = 7 and sum(non_p0_p1_runtime_top50_daily_label) = 7 then 1 else 0 end as non_p0_p1_runtime_top50_label
    from task_day_top_label
    group by task_table_name
),
job_current_day as (
    select *
    from job_daily_top_label
    where pt_date = '2026-06-01'
)
insert overwrite table test.ads_meta_yarn_job_calculation_label_di partition(pt_date = '2026-06-01')
select
    job_current_day.job_id,
    job_current_day.user_id,
    job_current_day.user_name,
    job_current_day.team_id,
    job_current_day.team_name,
    job_current_day.app_name,
    job_current_day.app_queue,
    job_current_day.app_type,
    case
        when lower(coalesce(job_current_day.app_type, '')) like '%spark%' or lower(coalesce(job_current_day.app_name, '')) like '%spark%' then 'Spark'
        when lower(coalesce(job_current_day.app_type, '')) like '%hive%' or lower(coalesce(job_current_day.app_name, '')) like '%hive%' then 'Hive'
        when lower(coalesce(job_current_day.app_type, '')) like '%flink%' or lower(coalesce(job_current_day.app_name, '')) like '%flink%' then 'Flink'
        when lower(coalesce(job_current_day.app_type, '')) like '%datax%' or lower(coalesce(job_current_day.app_name, '')) like '%datax%' then 'DataX'
        else 'Other'
    end as task_type_label,
    case when lower(coalesce(job_current_day.final_state, '')) = 'failed' then 1 else 0 end as task_failed_label,
    case
        when (lower(coalesce(job_current_day.app_type, '')) like '%spark%' or lower(coalesce(job_current_day.app_name, '')) like '%spark%')
             and coalesce(job_current_day.attempt_number, 0) > 0 then 1
        else 0
    end as spark_stage_failed_label,
    case
        when (lower(coalesce(job_current_day.app_type, '')) like '%spark%' or lower(coalesce(job_current_day.app_name, '')) like '%spark%')
             and coalesce(job_current_day.attempt_number, 0) > 3 then 1
        else 0
    end as spark_stage_failed_gt3_label,
    case
        when (lower(coalesce(job_current_day.app_type, '')) like '%spark%' or lower(coalesce(job_current_day.app_name, '')) like '%spark%')
             and coalesce(job_current_day.airflow_try_number, 0) > 0 then 1
        else 0
    end as spark_task_retry_label,
    case
        when (lower(coalesce(job_current_day.app_type, '')) like '%spark%' or lower(coalesce(job_current_day.app_name, '')) like '%spark%')
             and coalesce(job_current_day.airflow_try_number, 0) > 3 then 1
        else 0
    end as spark_task_retry_gt3_label,
    coalesce(top_streak.mu_top100_label, 0) as mu_top100_label,
    coalesce(top_streak.mu_top50_label, 0) as mu_top50_label,
    coalesce(top_streak.runtime_top100_label, 0) as runtime_top100_label,
    coalesce(top_streak.runtime_top50_label, 0) as runtime_top50_label,
    coalesce(top_streak.p0_p1_runtime_top100_label, 0) as p0_p1_runtime_top100_label,
    coalesce(top_streak.p0_p1_runtime_top50_label, 0) as p0_p1_runtime_top50_label,
    coalesce(top_streak.non_p0_p1_runtime_top100_label, 0) as non_p0_p1_runtime_top100_label,
    coalesce(top_streak.non_p0_p1_runtime_top50_label, 0) as non_p0_p1_runtime_top50_label,
    case when coalesce(job_current_day.elapsed_time_min, 0) > 60 then 1 else 0 end as runtime_gt_1h_label,
    case when coalesce(job_current_day.queue_wait_time, 0) > 10 then 1 else 0 end as submit_wait_gt_10min_label,
    case
        when job_current_day.avg_memory_allocate_gb is not null
             and job_current_day.input_size_gb is not null
             and job_current_day.avg_memory_allocate_gb < job_current_day.input_size_gb then 1
        else 0
    end as memory_alloc_insufficient_label,
    case
        when job_current_day.avg_memory_allocate_gb is not null
             and job_current_day.input_size_gb is not null
             and job_current_day.avg_memory_allocate_gb > job_current_day.input_size_gb * 2.5 then 1
        else 0
    end as memory_waste_label,
    case
        when job_current_day.gb_max_shuffle_read_size is not null
             and job_current_day.gb_avg_shuffle_read_size is not null
             and job_current_day.gb_avg_shuffle_read_size > 0
             and (job_current_day.gb_max_shuffle_read_size - job_current_day.gb_avg_shuffle_read_size) / job_current_day.gb_avg_shuffle_read_size > 2 then 1
        else 0
    end as data_skew_read_label,
    case
        when job_current_day.gb_max_shuffle_write_size is not null
             and job_current_day.gb_avg_shuffle_write_size is not null
             and job_current_day.gb_avg_shuffle_write_size > 0
             and (job_current_day.gb_max_shuffle_write_size - job_current_day.gb_avg_shuffle_write_size) / job_current_day.gb_avg_shuffle_write_size > 2 then 1
        else 0
    end as data_skew_write_label,
    case
        when job_current_day.major_gc_time is not null
             and job_current_day.task_time is not null
             and job_current_day.task_time > 0
             and job_current_day.major_gc_time / job_current_day.task_time > 0.2 then 1
        else 0
    end as full_gc_label,
    case
        when job_current_day.spill_disk_gb is not null and job_current_day.spill_disk_gb > 0 then 1
        else 0
    end as spill_disk_label,
    case
        when
            (case when lower(coalesce(job_current_day.final_state, '')) = 'failed' then 1 else 0 end) = 0
            and (case
                     when (lower(coalesce(job_current_day.app_type, '')) like '%spark%' or lower(coalesce(job_current_day.app_name, '')) like '%spark%')
                          and coalesce(job_current_day.attempt_number, 0) > 0 then 1
                     else 0
                 end) = 0
            and (case
                     when (lower(coalesce(job_current_day.app_type, '')) like '%spark%' or lower(coalesce(job_current_day.app_name, '')) like '%spark%')
                          and coalesce(job_current_day.attempt_number, 0) > 3 then 1
                     else 0
                 end) = 0
            and (case
                     when (lower(coalesce(job_current_day.app_type, '')) like '%spark%' or lower(coalesce(job_current_day.app_name, '')) like '%spark%')
                          and coalesce(job_current_day.airflow_try_number, 0) > 0 then 1
                     else 0
                 end) = 0
            and (case
                     when (lower(coalesce(job_current_day.app_type, '')) like '%spark%' or lower(coalesce(job_current_day.app_name, '')) like '%spark%')
                          and coalesce(job_current_day.airflow_try_number, 0) > 3 then 1
                     else 0
                 end) = 0
            and (case when coalesce(top_streak.mu_top100_label, 0) = 1 then 1 else 0 end) = 0
            and (case when coalesce(top_streak.mu_top50_label, 0) = 1 then 1 else 0 end) = 0
            and (case when coalesce(top_streak.runtime_top100_label, 0) = 1 then 1 else 0 end) = 0
            and (case when coalesce(top_streak.runtime_top50_label, 0) = 1 then 1 else 0 end) = 0
            and (case when coalesce(top_streak.p0_p1_runtime_top100_label, 0) = 1 then 1 else 0 end) = 0
            and (case when coalesce(top_streak.p0_p1_runtime_top50_label, 0) = 1 then 1 else 0 end) = 0
            and (case when coalesce(top_streak.non_p0_p1_runtime_top100_label, 0) = 1 then 1 else 0 end) = 0
            and (case when coalesce(top_streak.non_p0_p1_runtime_top50_label, 0) = 1 then 1 else 0 end) = 0
            and (case
                     when job_current_day.avg_memory_allocate_gb is not null
                          and job_current_day.input_size_gb is not null
                          and job_current_day.avg_memory_allocate_gb > job_current_day.input_size_gb then 1
                     else 0
                 end) = 0
            and (case
                     when job_current_day.gb_max_shuffle_read_size is not null
                          and job_current_day.gb_avg_shuffle_read_size is not null
                          and job_current_day.gb_avg_shuffle_read_size > 0
                          and (job_current_day.gb_max_shuffle_read_size - job_current_day.gb_avg_shuffle_read_size) / job_current_day.gb_avg_shuffle_read_size > 2 then 1
                     else 0
                 end) = 0
            and (case
                     when job_current_day.gb_max_shuffle_write_size is not null
                          and job_current_day.gb_avg_shuffle_write_size is not null
                          and job_current_day.gb_avg_shuffle_write_size > 0
                          and (job_current_day.gb_max_shuffle_write_size - job_current_day.gb_avg_shuffle_write_size) / job_current_day.gb_avg_shuffle_write_size > 2 then 1
                     else 0
                 end) = 0
            and (case
                     when job_current_day.major_gc_time is not null
                          and job_current_day.task_time is not null
                          and job_current_day.task_time > 0
                          and job_current_day.major_gc_time / job_current_day.task_time > 0.2 then 1
                     else 0
                 end) = 0
            and (case
                     when job_current_day.spill_disk_gb is not null and job_current_day.spill_disk_gb > 0 then 1
                     else 0
                 end) = 0
        then 1
        else 0
    end as task_health_label,
    concat(
        '[',
        hour(started_date),
        ', ',
        hour(started_date) + 1,
        ')'
    ) AS started_hour_bucket
from job_current_day
left join task_7d_top_streak top_streak
    on job_current_day.task_table_name = top_streak.task_table_name
;

