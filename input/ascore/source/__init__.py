from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from dwh.common.sensors.portal_dqc import PortalDqcSensorAsync
from dwh.common.skip import skip_if_execution_date_not_match
from dwh.credit_risk.dwh_credit_risk.task.bmart_udl_risk.channel_loan_credit_risk_tmp import bmart_udl_risk__channel_loan_credit_risk_tmp
from dwh.credit_risk.dwh_credit_risk.task.dws.t80_dim_time_cs_d import dws__t80_dim_time_cs_d
from dwh.credit_risk.dwh_credit_risk.task.fmart_loan.dwd_loan_accounting_dc import fmart_loan__dwd_loan_accounting_dc
from dwh.credit_risk.dwh_credit_risk.task.fmart_loan.dwd_loan_application_dc import fmart_loan__dwd_loan_application_dc

dm__credit_risk_ph_ascore_loanstatus_offline_v2_ss_d = TaskGroup(
    group_id='dm__credit_risk_ph_ascore_loanstatus_offline_v2_ss_d.task_group',
    prefix_group_id=False,
)

with dm__credit_risk_ph_ascore_loanstatus_offline_v2_ss_d:
    data = BashOperator(
        task_id="dm__credit_risk_ph_ascore_loanstatus_offline_v2_ss_d.data",
        pre_execute=skip_if_execution_date_not_match(day=-1),
        bash_command='spark-submit '
                     '--master yarn '
                     '--deploy-mode cluster '
                     '--queue warehouse '
                     '--name {job_name} '
                     '{spark_parameters} '
                     '--conf spark.yarn.dist.archives=hdfs:///user/bi/regriskds/dist/con_env_withpyarrow.tar.gz#environment '
                     '--conf spark.pyspark.python=./environment/bin/python3 '
                     '--py-files {py_files} '
                     '{file_path} "{dt}" "{env}" "{db_prefix}"'.format(
            job_name="dm__credit_risk_ph_ascore_loanstatus_offline_v2_ss_d",
            spark_parameters="{{ var.value.dm_spark_parameters }}",
            py_files="/home/hadoop/airflow/dags/dwh/credit_risk/dwh_credit_risk/task/dm/CREDIT_RISK_v2_ascore_pkgs.zip",
            file_path="/home/hadoop/airflow/dags/dwh/credit_risk/dwh_credit_risk/task/dm/credit_risk_ph_ascore_loanstatus_offline_v2_ss_d/loanstatus_offline_prod_hdfs.py",
            dt="{{ next_ds }}",
            env="{{ var.value.env.lower() }}",
            db_prefix="{{ var.value.db_prefix }}"
        )
    )

    dqc = PortalDqcSensorAsync(
        task_id='dm__credit_risk_ph_ascore_loanstatus_offline_v2_ss_d',
        pre_execute=skip_if_execution_date_not_match(day=-1),
        schema='{{ var.value.db_prefix }}dm',
        table='credit_risk_ph_ascore_loanstatus_offline_v2_ss_d',
        mode='reschedule',
        pool='sensor_pool',
    )

    data >> dqc

dws__t80_dim_time_cs_d >> dm__credit_risk_ph_ascore_loanstatus_offline_v2_ss_d
fmart_loan__dwd_loan_accounting_dc >> dm__credit_risk_ph_ascore_loanstatus_offline_v2_ss_d
fmart_loan__dwd_loan_application_dc >> dm__credit_risk_ph_ascore_loanstatus_offline_v2_ss_d
bmart_udl_risk__channel_loan_credit_risk_tmp >> dm__credit_risk_ph_ascore_loanstatus_offline_v2_ss_d
