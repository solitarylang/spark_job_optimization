import datetime
import functools
import operator
import sys

import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GeneralizedLinearRegressionModel


def loanstatus_module(dwh_date, run_environment, db_prefix):
    from util_helper import spark_helper
    from util_helper.woe_helper_hdfs import WOEtransformer

    shelper = spark_helper.PHBankSpark(app_name="zx_prod_loanstatus",
                                       config_dict={'spark.executor.cores': 4,
                                                    'spark.executor.instances': 32,
                                                    'spark.dynamicAllocation.maxExecutors': 32,
                                                    'spark.dynamicAllocation.enabled': False,
                                                    'spark.executor.memory': '16g',
                                                    'spark.executor.memoryOverhead': '2g'})

    ############################### NEED TO UPDATE for the next model update ##########################################
    # configurations on woe and model path depends on the production or live environment:
    woe_run_date = "2024_01_22"
    if run_environment == "live":
        woe_pkl_path = "hdfs://phlive1//regriskds/ph_ascore/model/loanstatus_0dpd_etb_woe_rundate=2024_01_22.pkl"  # hdfs
        model_parquet_path = 'hdfs://phlive1//regriskds/ph_ascore/model/'
    elif run_environment == "uat":
        woe_pkl_path = "hdfs://nptuat/user/bi/regriskds/ph_ascore/model/woe/loanstatus_0dpd_etb_woe_rundate=2024_01_22.pkl"  # hdfs
        model_parquet_path = 'hdfs://nptuat/user/bi/regriskds/ph_ascore/model/model/'

    ############################### NEED TO UPDATE for the next model update ##########################################
    # configuration for model parameters
    # model_parquet_path = 'hdfs://phlive1/user/bi/regriskds/ph_ascore/model/'
    model_name = 'BaselineLogisticRegressionClassifier'
    module = 'loanstatus'
    dpd = 'etb'
    model_run_date = '2024_01_22'

    # the model run date is one day after then dwh_date
    run_date = (datetime.datetime.strptime(dwh_date, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    # define feature extration date (today's date)
    feature_datetime = datetime.datetime.strptime(run_date, "%Y-%m-%d") + relativedelta(months=-1) + relativedelta(
        day=31)
    feature_date = feature_datetime.strftime('%Y-%m-%d')

    spark = shelper.build_session()  # start a spark session. remember to kill it once data extraction is done

    feature_datetime = (datetime.datetime.strptime(feature_date, "%Y-%m-%d"))
    dates_of_interest = []
    for i in [4, 3, 2, 1, 0]:
        mend_date = feature_datetime + relativedelta(months=-i) + relativedelta(day=31)
        dates_of_interest.append(mend_date.strftime('%Y-%m-%d'))
    sample_target_dates = sorted(set(dates_of_interest + [feature_date]))
    target_sample_dates_sql = ", ".join([f"DATE('{d}')" for d in sample_target_dates])

    # # Step 1: Feature Generation

    # ## Step 1.0: for loanstatus module
    # * ft_count_xdpd_sum_3m
    # * ft_has_7dpd_3m
    # * ft_has_7dpd_2m
    # * ft_disbursement_amount_sum_1m
    # * ft_disbursement_amount_sum_3m
    # * ft_disbursement_count_max_3m
    # * ft_pct_xdpd_max_2m
    # * ft_loanappl_count_fail_sum_3m

    # ## Step 1.1: for loan delinquency information

    sdf_credit_all = spark.sql(f"""
         select
            loan_no,
            prod_type,
            client_no,
            account_no,
            outstanding_principal,
            outstanding_interest,
            outstanding_fee,
            tenor,
            product_type,
            current_tenor_no,
            loan_status,
            accounting_status,
            maturity_date,
            pt_date,
            loan_disbursement_date,
            day_past_due,
            loan_disb
         from {db_prefix}bmart_udl_risk.channel_loan_credit_risk_tmp
         where pt_date >= date_format(DATE('{feature_date}') - INTERVAL '130' DAY, 'yyyy-MM-dd')
           and pt_date <= '{feature_date}'
           and prod_type in ('101', '102', '120')
        """)

    # FROM_UNIXTIME(UNIX_TIMESTAMP(pt_date, 'yyyy-MM-dd')) AS pt_date,

    sdf_credit_all = sdf_credit_all.withColumn('cur_balance', F.col('outstanding_principal') +
                                               F.col('outstanding_interest') +
                                               F.col('outstanding_fee'))

    # aggregate daily data to monthly at loan_no level
    sdf_dpd_monthly = sdf_credit_all.groupBy(
        "client_no", "loan_no", F.last_day(F.col("pt_date")).alias("pt_date")).agg(
        F.max("day_past_due").alias("day_past_due")
    )  # aggregate to get monthly data on balance
    # the filter cur_balance > 0 is to exclude settled loans
    sdf_loan_monthly = sdf_credit_all.withColumn("pt_date", F.last_day(F.col("pt_date")))
    sdf_loan_monthly = sdf_loan_monthly.filter(F.col('cur_balance') > 0).groupBy("client_no", "pt_date").agg(
        F.avg("cur_balance").alias("loan_os_mtd"))

    lbs = {"label_xdpd": 1,
           "label_7dpd": 8,
           "label_15dpd": 16,
           "label_30dpd": 31
           }

    sdf_dpd_monthly = sdf_dpd_monthly.select(
        *sdf_dpd_monthly.columns,
        F.lit(1).alias("label_0dpd"))

    for k, v in lbs.items():
        sdf_dpd_monthly = sdf_dpd_monthly.select(
            *sdf_dpd_monthly.columns,
            F.when(F.col("day_past_due") >= v, 1).otherwise(0).alias(k)
        )

    sdf_dpd_monthly.printSchema()

    # transpose to client_no level and pivot by pt_date
    sdf_client_dpd_monthly = sdf_dpd_monthly.groupBy(
        "client_no").pivot(
        "pt_date", values=dates_of_interest).agg(
        F.sum(F.col("label_0dpd")).alias("count_0dpd"),
        F.sum(F.col("label_xdpd")).alias("count_xdpd"),
        F.sum(F.col("label_7dpd")).alias("count_7dpd"),
        F.sum(F.col("label_15dpd")).alias("count_15dpd"),
        F.sum(F.col("label_30dpd")).alias("count_30dpd"),
    )

    sdf_client_dpd_monthly = sdf_client_dpd_monthly.select(
        *sdf_client_dpd_monthly.columns + [
            (F.col(f"{d}_count_{dpd}") / F.col(f"{d}_count_0dpd")).alias(f"{d}_pct_{dpd}")
            for dpd in ["xdpd", "7dpd", "15dpd", "30dpd"] for d in dates_of_interest]
    )

    sdf_client_dpd_monthly.printSchema()

    ### helper functions
    def generate_ever_dpd(pt_date, num_months, dpd):
        # returns a column
        # referencing pt_date, check whether in it and the past num_months-1, has the loan exceed dpd

        condition = None
        d = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        for i in range(num_months):
            if condition is None:
                condition = F.when(F.col(f"{d.strftime('%Y-%m-%d')}_count_{dpd}") > 0, 1)
            else:
                condition = condition.when(F.col(f"{d.strftime('%Y-%m-%d')}_count_{dpd}") > 0, 1)
            d = d + relativedelta(months=-1) + relativedelta(day=31)
        condition = condition.otherwise(0)
        return condition

    def generate_maximum_dpd(pt_date, num_months, value, dpd):
        # returns a column
        # referencing pt_date, check in it and each of the past num_months-1, the maximum number of loans exceeding dpd

        d = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        if num_months == 1:
            condition = F.col(f"{d.strftime('%Y-%m-%d')}_{value}_{dpd}")
        else:
            condition = F.greatest(
                *[F.col(f"{(d + relativedelta(months=-i) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}")
                  for i in range(num_months)])
        return condition

    def generate_minimum_dpd(pt_date, num_months, value, dpd):
        # returns a column
        # referencing pt_date, check in it and each of the past num_months-1, the minimum number of loans exceeding dpd

        d = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        if num_months == 1:
            condition = F.col(f"{d.strftime('%Y-%m-%d')}_{value}_{dpd}")
        else:
            condition = F.least(
                *[F.col(f"{(d + relativedelta(months=-i) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}")
                  for i in range(num_months)])
        return condition

    def generate_sum_dpd(pt_date, num_months, value, dpd):
        # returns a column
        # referencing pt_date, check in it and each of the past num_months-1, the sum number of loans exceeding dpd

        d = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        if num_months == 1:
            condition = F.col(f"{d.strftime('%Y-%m-%d')}_{value}_{dpd}")
        else:
            ################################### !!!! add logic to deal with empty string ###############################
            condition = functools.reduce(
                operator.add,
                [F.when(F.coalesce(F.col(
                    f"{(d + relativedelta(months=-i) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}"),
                    F.lit(0)) == "", F.lit(0)).otherwise(
                    F.coalesce(F.col(
                        f"{(d + relativedelta(months=-i) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}"),
                        F.lit(0)))
                    for i in range(num_months)])
            ############################################################################################################
        return condition

    def generate_change_dpd(pt_date, num_months, value, dpd, dates_of_interest):
        # returns a column
        # referencing pt_date, check in it and each of the past num_months-1, the maximum number of loans exceeding dpd

        d = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        if (d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime(
                '%Y-%m-%d') not in dates_of_interest:
            change = F.lit(0)
        else:
            ################################### !!!! add logic to deal with empty string ###############################
            change = F.when(F.coalesce(F.col(
                f"{(d + relativedelta(months=-num_months + 1) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}") - \
                                       F.col(
                                           f"{(d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}"),
                                       F.lit(0)) == "", F.lit(0)).otherwise(
                F.coalesce(F.col(
                    f"{(d + relativedelta(months=-num_months + 1) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}") - \
                           F.col(
                               f"{(d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}"),
                           F.lit(0)))
            ############################################################################################################
        return change

    def generate_chg_ratio_dpd(pt_date, num_months, value, dpd, dates_of_interest):
        # returns a column
        # referencing pt_date, check in it and each of the past num_months-1, the change ratio number of loans exceeding dpd

        d = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        if (d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime(
                '%Y-%m-%d') not in dates_of_interest:
            change_ratio = F.lit(0)
        else:
            ################################### !!!! add logic to deal with empty string ###############################
            change_ratio = F.when(F.coalesce(F.col(
                f"{(d + relativedelta(months=-num_months + 1) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}") \
                                             / F.col(
                f"{(d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}") - 1,
                                             F.lit(0)) == "", F.lit(0)).otherwise(
                F.coalesce(F.col(
                    f"{(d + relativedelta(months=-num_months + 1) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}") \
                           / F.col(
                    f"{(d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{value}_{dpd}") - 1,
                           F.lit(0)))
            ############################################################################################################

        return change_ratio

    # ever dpd indicator
    sdf_client_ever_dpd = None

    d = (feature_datetime + relativedelta(months=0) + relativedelta(day=31)).strftime('%Y-%m-%d')
    sdf = sdf_client_dpd_monthly.select(
        "client_no",
        F.lit(d).alias("pt_date"),
        *[generate_ever_dpd(d, num_month, dpd).alias(f"ft_has_{dpd}_{num_month}m") for num_month in [1, 2, 3] for dpd in
          ["xdpd", "7dpd", "15dpd", "30dpd"]])

    sdf_client_ever_dpd = sdf

    sdf_client_ever_dpd.printSchema()

    # count dpd loans
    sdf_client_count_dpd = None

    d = (feature_datetime + relativedelta(months=0) + relativedelta(day=31)).strftime('%Y-%m-%d')
    sdf = sdf_client_dpd_monthly.select(
        "client_no",
        F.lit(d).alias("pt_date"),
        *[generate_maximum_dpd(d, num_month, 'count', dpd).alias(f"ft_count_{dpd}_max_{num_month}m") for num_month in
          [1, 2, 3] for dpd in ["xdpd", "7dpd", "15dpd", "30dpd"]],
        *[generate_change_dpd(d, num_month, 'count', dpd, dates_of_interest).alias(
            f"ft_count_{dpd}_change_{num_month}m")
            for num_month in [1, 2, 3] for dpd in ["xdpd", "7dpd", "15dpd", "30dpd"]],
        *[generate_minimum_dpd(d, num_month, 'count', dpd).alias(f"ft_count_{dpd}_min_{num_month}m") for num_month in
          [1, 2, 3] for dpd in ["xdpd", "7dpd", "15dpd", "30dpd"]],
        *[generate_sum_dpd(d, num_month, 'count', dpd).alias(f"ft_count_{dpd}_sum_{num_month}m") for num_month in
          [1, 2, 3] for dpd in ["xdpd", "7dpd", "15dpd", "30dpd"]],
        *[generate_chg_ratio_dpd(d, num_month, 'count', dpd, dates_of_interest).alias(
            f"ft_count_{dpd}_change_ratio_{num_month}m") for num_month in [1, 2, 3] for dpd in
            ["xdpd", "7dpd", "15dpd", "30dpd"]]
    )

    sdf_client_count_dpd = sdf

    # pct dpd loans
    sdf_client_pct_dpd = None

    d = (feature_datetime + relativedelta(months=0) + relativedelta(day=31)).strftime('%Y-%m-%d')
    sdf = sdf_client_dpd_monthly.select(
        "client_no",
        F.lit(d).alias("pt_date"),
        *[generate_maximum_dpd(d, num_month, 'pct', dpd).alias(f"ft_pct_{dpd}_max_{num_month}m") for num_month in
          [1, 2, 3] for dpd in ["xdpd", "7dpd", "15dpd", "30dpd"]],
        *[generate_change_dpd(d, num_month, 'pct', dpd, dates_of_interest).alias(f"ft_pct_{dpd}_change_{num_month}m")
          for num_month in [1, 2, 3] for dpd in ["xdpd", "7dpd", "15dpd", "30dpd"]],
        *[generate_minimum_dpd(d, num_month, 'pct', dpd).alias(f"ft_pct_{dpd}_min_{num_month}m") for num_month in
          [1, 2, 3] for dpd in ["xdpd", "7dpd", "15dpd", "30dpd"]],
        *[generate_sum_dpd(d, num_month, 'pct', dpd).alias(f"ft_pct_{dpd}_sum_{num_month}m") for num_month in [1, 2, 3]
          for dpd in ["xdpd", "7dpd", "15dpd", "30dpd"]],
        *[generate_chg_ratio_dpd(d, num_month, 'pct', dpd, dates_of_interest).alias(
            f"ft_pct_{dpd}_change_ratio_{num_month}m") for num_month in [1, 2, 3] for dpd in
            ["xdpd", "7dpd", "15dpd", "30dpd"]]
    )

    sdf_client_pct_dpd = sdf

    # combine loan dpd features
    sdf_ft_loan_dpd = sdf_client_ever_dpd.join(
        sdf_client_count_dpd, on=["client_no", "pt_date"]).join(
        sdf_client_pct_dpd, on=["client_no", "pt_date"])

    sdf_ft_loandelq = sdf_ft_loan_dpd

    sdf_ft_loandelq.printSchema()

    # ## Step 1.2: loan application features

    sdf_loan_appl = spark.sql(f"""
          select 
    tt1.client_no,
    tt1.loan_no,
    tt1.apply_date apply_date,
    tt1.apply_status apply_status,
    tt2.pt_date as pt_date
    from
        (select 
             CASE WHEN dc_start_date <= (DATE('{feature_date}') - INTERVAL '130' DAY)
                 THEN (DATE('{feature_date}') - INTERVAL '130' DAY)
                   ELSE dc_start_date
                     END AS start_date,
            dc_end_date AS end_date,
            client_no,
            loan_no,
            date_format(from_unixtime(cast(application_timestamp as bigint) / 1000), 'yyyy-MM-dd') apply_date,
            status apply_status
            from {db_prefix}fmart_loan.dwd_loan_application_dc
             WHERE  dc_start_date <=  (DATE('{feature_date}') )
             AND dc_end_date >  (DATE('{feature_date}') - INTERVAL '130' DAY)) tt1
            left join
                    (select
                        calendar_date as pt_date
                    from {db_prefix}dws.t80_dim_time_cs_d
                    where calendar_date>= (DATE('{feature_date}') - INTERVAL '130' DAY)
                    and calendar_date<=(DATE('{feature_date}') ))tt2
                on 1=1
                WHERE tt2.pt_date >= tt1.start_date and tt2.pt_date < tt1.end_date
    """)

    ############################################## To remove in the retrained model Jan 2024###########################
    """# take the latest record for each loan
    w = Window.partitionBy("client_no", "loan_no").orderBy(F.col("pt_date").desc())
    sdf_loan_appl = sdf_loan_appl.withColumn(
        "row", F.row_number().over(w)).filter(
        F.col("row") == 1).drop("row")"""
    ############################################## To remove in the retrained model Jan 2024###########################

    sdf_loan_appl.printSchema()

    # only take failed applications
    sdf_loan_appl = sdf_loan_appl.withColumn("pt_date",
                                             F.last_day(F.to_date(F.col("apply_date").cast("string"), "yyyyMMdd")))
    sdf_loan_appl = sdf_loan_appl.filter(F.col("apply_status").isin(["LOAN_FAIL"]))
    sdf_loan_appl = sdf_loan_appl.withColumn("failed_appl", F.lit(1))

    sdf_appl_client = sdf_loan_appl.groupBy("client_no").pivot("pt_date"). \
        agg(F.count(F.col("failed_appl")).alias("ft_loanappl_count_fail"),
            F.max(F.col("failed_appl")).alias("ft_loanappl_ind_fail"))

    sdf_appl_client.printSchema()

    # to deal with the month that do not have data
    for date in dates_of_interest:
        count_fail_col = f"{date}_ft_loanappl_count_fail"
        ind_fail_col = f"{date}_ft_loanappl_ind_fail"
        if count_fail_col not in sdf_appl_client.columns:
            sdf_appl_client = sdf_appl_client.withColumn(count_fail_col, F.lit(None))
        if ind_fail_col not in sdf_appl_client.columns:
            sdf_appl_client = sdf_appl_client.withColumn(ind_fail_col, F.lit(None))

    sdf_appl_client.printSchema()

    ### helper functions
    def generate_maximum_feature(pt_date, num_months, suffix):
        # returns a column
        # referencing pt_date, check in it and each of the past num_months-1, the maximum of numbers

        d = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        if num_months == 1:
            condition = F.col(f"{d.strftime('%Y-%m-%d')}_{suffix}")
        else:
            condition = F.greatest(
                *[F.col(f"{(d + relativedelta(months=-i) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}") for i
                  in range(num_months)])
        return condition

    def generate_minimum_feature(pt_date, num_months, suffix):
        # returns a column
        # referencing pt_date, check in it and each of the past num_months-1, the minimum of numbers

        d = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        if num_months == 1:
            condition = F.col(f"{d.strftime('%Y-%m-%d')}_{suffix}")
        else:
            condition = F.least(
                *[F.col(f"{(d + relativedelta(months=-i) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}") for i
                  in range(num_months)])
        return condition

    def generate_sum_feature(pt_date, num_months, suffix):
        # returns a column
        # referencing pt_date, check in it and each of the past num_months-1, the sum of numbers

        d = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        if num_months == 1:
            condition = F.when(F.coalesce(F.col(f"{d.strftime('%Y-%m-%d')}_{suffix}"), F.lit(0)) == "",
                               F.lit(0)).otherwise(
                F.coalesce(F.col(f"{d.strftime('%Y-%m-%d')}_{suffix}"), F.lit(0)))
        else:
            ################################### !!!! add logic to deal with empty string ###############################
            condition = functools.reduce(
                operator.add,
                [F.when(F.coalesce(
                    F.col(f"{(d + relativedelta(months=-i) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}"),
                    F.lit(0)) == "", F.lit(0)).otherwise(
                    F.coalesce(F.col(
                        f"{(d + relativedelta(months=-i) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}"),
                        F.lit(0)))
                    for i in range(num_months)])
            ############################################################################################################
        return condition

    def generate_change_feature(pt_date, num_months, suffix, dates_of_interest):
        # returns a column
        # referencing pt_date, check in it and each of the past num_months-1, the minimum of numbers
        d = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        if not ((d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime(
                '%Y-%m-%d') in dates_of_interest):
            change = F.lit(0)
        else:
            ################################### !!!! add logic to deal with empty string ###############################
            change = F.when(F.coalesce(F.col(
                f"{(d + relativedelta(months=-num_months + 1) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}") \
                                       - F.col(
                f"{(d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}"),
                                       F.lit(0)) == "", F.lit(0)).otherwise(
                F.coalesce(F.col(
                    f"{(d + relativedelta(months=-num_months + 1) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}") \
                           - F.col(
                    f"{(d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}"),
                           F.lit(0)))
            ############################################################################################################
        return change

    def generate_chg_ratio_feature(pt_date, num_months, suffix, dates_of_interest):
        # returns a column
        # referencing pt_date, check in it and each of the past num_months-1, the minimum of numbers
        d = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        if (d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime(
                '%Y-%m-%d') not in dates_of_interest:
            change = F.lit(0)
        else:
            ################################### !!!! add logic to deal with empty string ###############################
            change = F.when(F.coalesce((F.col(
                f"{(d + relativedelta(months=-num_months + 1) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}") \
                                        / F.col(
                        f"{(d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}")) - F.lit(
                1), F.lit(0)) == "", F.lit(0)).otherwise(
                F.coalesce((F.col(
                    f"{(d + relativedelta(months=-num_months + 1) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}") \
                            / F.col(
                            f"{(d + relativedelta(months=-num_months) + relativedelta(day=31)).strftime('%Y-%m-%d')}_{suffix}")) - F.lit(
                    1), F.lit(0)))
            ############################################################################################################
        return change

    # disbursement count, amount
    sdf_ft_loan_appl = None

    d = (feature_datetime + relativedelta(months=0) + relativedelta(day=31)).strftime('%Y-%m-%d')
    sdf = sdf_appl_client.select(
        "client_no",
        F.lit(d).alias("pt_date"),
        *[generate_maximum_feature(d, num_month, "ft_loanappl_count_fail").alias(
            f"ft_loanappl_count_fail_max_{num_month}m") for num_month in [1, 2, 3]],
        *[generate_sum_feature(d, num_month, "ft_loanappl_count_fail").alias(f"ft_loanappl_count_fail_sum_{num_month}m")
          for num_month in [1, 2, 3]],
        *[generate_change_feature(d, num_month, "ft_loanappl_count_fail", dates_of_interest).alias(
            f"ft_loanappl_count_fail_chg_{num_month}m") for num_month in [1, 2, 3]],
        *[generate_chg_ratio_feature(d, num_month, "ft_loanappl_count_fail", dates_of_interest).alias(
            f"ft_loanappl_count_fail_chg_ratio_{num_month}m") for num_month in [1, 2, 3]],
        *[generate_maximum_feature(d, num_month, "ft_loanappl_ind_fail").alias(f"ft_loanappl_ind_fail_max_{num_month}m")
          for num_month in [1, 2, 3]]
    )

    if sdf_ft_loan_appl is None:
        sdf_ft_loan_appl = sdf.alias("sdf_ft_loan_appl")
    else:
        sdf_ft_loan_appl = sdf_ft_loan_appl.union(sdf)

    sdf_ft_loan_appl.printSchema()

    # sdf_ft_loan_appl.limit(5).toPandas()

    # ## Step 1.3: loan disbursement features

    sdf_loan_disbursement_monthly = sdf_credit_all.select(
        "client_no", "loan_no", "product_type", "tenor", "loan_disb",
        F.last_day(F.col("loan_disbursement_date")).alias("pt_date")).dropDuplicates()

    # transpose to client_no level and pivot by pt_date
    sdf_client_disbursement_monthly = sdf_loan_disbursement_monthly.groupBy(
        "client_no").pivot(
        "pt_date", values=dates_of_interest).agg(
        F.count(F.col("loan_no")).alias("loan_disbursement_count"),
        F.sum(F.col("loan_disb")).alias("loan_disbursement_amount"))

    # disbursement count, amount
    sdf_ft_loan_disbursement = None

    d = (feature_datetime + relativedelta(months=0) + relativedelta(day=31)).strftime('%Y-%m-%d')
    sdf = sdf_client_disbursement_monthly.select(
        "client_no",
        F.lit(d).alias("pt_date"),
        *[generate_maximum_feature(d, num_month, "loan_disbursement_count").alias(
            f"ft_disbursement_count_max_{num_month}m") for num_month in [1, 2, 3]],
        *[generate_sum_feature(d, num_month, "loan_disbursement_count").alias(f"ft_disbursement_count_sum_{num_month}m")
          for num_month in [1, 2, 3]],
        *[generate_change_feature(d, num_month, "loan_disbursement_count", dates_of_interest).alias(
            f"ft_disbursement_count_chg_{num_month}m") for num_month in [1, 2, 3]],
        *[generate_chg_ratio_feature(d, num_month, "loan_disbursement_count", dates_of_interest).alias(
            f"ft_disbursement_count_chg_ratio_{num_month}m") for num_month in [1, 2, 3]],
        *[generate_maximum_feature(d, num_month, "loan_disbursement_amount").alias(
            f"ft_disbursement_amount_max_{num_month}m") for num_month in [1, 2, 3]],
        *[generate_sum_feature(d, num_month, "loan_disbursement_amount").alias(
            f"ft_disbursement_amount_sum_{num_month}m") for num_month in [1, 2, 3]],
        *[generate_change_feature(d, num_month, "loan_disbursement_amount", dates_of_interest).alias(
            f"ft_disbursement_amount_chg_{num_month}m") for num_month in [1, 2, 3]],
        *[generate_chg_ratio_feature(d, num_month, "loan_disbursement_amount", dates_of_interest).alias(
            f"ft_disbursement_amount_chg_ratio_{num_month}m") for num_month in [1, 2, 3]],
    )

    sdf_ft_loan_disbursement = sdf

    sdf_ft_loan_disbursement.printSchema()

    # ## Step 1.4: create sample and mob

    sdf_loan_receipt = spark.sql(f"""
        SELECT
            client_no,
            tenor_count tenor,
            loan_status,
            accounting_status,
            maturity_date AS maturity_date,
            disbursement_accounting_date AS disburse_date,
            dpd_current_max as dpd,
            pt_date AS pt_date
        FROM {db_prefix}fmart_loan.dwd_loan_accounting_df
        WHERE repayment_status <> 'REFUND'
          AND sub_prod_code IN ('101', '102', '120')
          AND pt_date IN ({target_sample_dates_sql})
        """)

    # align loan_disbursement_date and pt_date to month end
    sdf_loan_rcpt_mthend = sdf_loan_receipt.withColumn("disburse_date",
                                                       F.last_day(sdf_loan_receipt['disburse_date']))
    ##############################################################################################################
    # only keep the sample on the rundate previous one day
    # !!!! change to feature date as the dwh_date is the month beginining like 2024-01-01,
    # !!!! However the data for 2024-01-01 is not available at the day of 2024-01-01
    sdf_loan_rcpt_mthend = sdf_loan_rcpt_mthend.filter(F.col("pt_date") == feature_date)
    ##############################################################################################################

    # get client_no level first loan_disbursement_month
    client_start_date = sdf_loan_rcpt_mthend.groupby("client_no").agg(
        F.min(F.last_day("disburse_date")).alias("client_start_date"))

    sdf_loan_rcpt_mthend = sdf_loan_rcpt_mthend.join(client_start_date, on="client_no")
    ##############################################################################################################
    # !!!! though the pt_date based on the table date which is the previous month end as the loan features
    # !!!! however the customer come in one month later, therefore the mob need to be plus 1
    sdf_loan_rcpt_mthend = sdf_loan_rcpt_mthend.withColumn("mob", F.months_between(F.last_day(F.col("pt_date")),
                                                                                   F.last_day(
                                                                                       F.col("client_start_date"))) + 1)
    ##############################################################################################################
    sdf_loan_rcpt_mthend = sdf_loan_rcpt_mthend.filter(F.col("mob") >= 0)

    # ### to get the previous month end xdpd and 0dpd

    sdf_credit_all_mend = sdf_loan_receipt.filter(F.col("pt_date") == F.last_day("pt_date"))
    sdf_credit_all_mend = sdf_credit_all_mend.groupBy("client_no", "pt_date").agg(F.max("dpd").alias("dpd_pre_mend"))

    sdf_credit_all_mend = sdf_credit_all_mend.withColumn("pt_date-1m", F.col("pt_date"))
    sdf_credit_all_mend = sdf_credit_all_mend.withColumnRenamed("pt_date", "pt_date_pre_mend")

    ##############################################################################################################
    # !!!! though the pt_date based on the table date which is the previous month end as the loan features
    # !!!! thus the loan previous month end is the pt_date
    sdf_loan_rcpt_mthend = sdf_loan_rcpt_mthend.withColumn("pt_date-1m", F.last_day(F.add_months(("pt_date"), 0)))
    ##############################################################################################################
    sdf_all = sdf_loan_rcpt_mthend.join(sdf_credit_all_mend, on=["client_no", "pt_date-1m"], how="left")
    sdf_all = sdf_all.withColumn("seg_obs", F.when(F.col("dpd_pre_mend") > 0, "xdpd").otherwise("0dpd"))

    # define the feature date as the pt date to join with feature tables
    sdf_all = sdf_all.select("client_no", F.lit(feature_date).alias("pt_date"), "mob", "seg_obs")
    sdf_all = sdf_all.dropDuplicates()
    sdf_all.printSchema()

    # join with features for loan status
    sdf_sample_loanstatus = sdf_all.join(
        sdf_ft_loandelq, on=["client_no", "pt_date"], how="left").join(
        sdf_ft_loan_appl, on=["client_no", "pt_date"], how="left").join(
        sdf_ft_loan_disbursement, on=["client_no", "pt_date"], how="left")

    # only keep the model final features on balance amount
    ft_keep = ["client_no", "pt_date", "mob", "seg_obs",
               "ft_count_xdpd_sum_2m",
               "ft_count_xdpd_max_1m",
               "ft_disbursement_amount_max_2m",
               "ft_disbursement_count_max_3m",
               "ft_count_7dpd_sum_3m",
               "ft_pct_xdpd_change_3m",
               "ft_count_15dpd_sum_3m",
               "ft_count_7dpd_sum_1m",
               "ft_count_xdpd_change_3m",
               "ft_count_xdpd_change_ratio_1m"
               ]

    sdf_sample_loanstatus = sdf_sample_loanstatus.select(ft_keep)

    sdf_sample_loanstatus.printSchema()

    #####################################!!!!! fill na with 0 ###########################################################
    # fillna with 0
    fts_fillna = [ft for ft in sdf_sample_loanstatus.columns if "ft" in ft]
    sdf_sample_loanstatus = sdf_sample_loanstatus.fillna(0, subset=fts_fillna)
    ###################################################################################################################

    print("Step 1 Done: for feature extraction")

    # sdf_sample_loanstatus.limit(5).toPandas()

    # # Step 2: WOE Encoding

    # reload woe pickle file
    woe_transformer_major = WOEtransformer()
    woe_transformer_major.load(woe_pkl_path, spark)

    ############################################## To remove in the retrained model Jan 2024###########################
    """transformer_bin_dict = woe_transformer_major.transformer_bin_dict.copy()
    transformer_woe_dict = woe_transformer_major.transformer_woe_dict.copy()
    mapping_transformation = woe_transformer_major.mapping_transformation

    for ifeature in transformer_bin_dict.keys():
        # Assign groupID of -2 to values that not shown in the training sample
        is_dummy = f' ELSE {ifeature} ' in transformer_bin_dict[ifeature]

        if is_dummy:
            feature_mapping = mapping_transformation[mapping_transformation['Feature'] == ifeature]
            condition = ''
            for ivalue in feature_mapping['Value']:
                if ivalue != 'Missing':
                    condition += f'WHEN ({ifeature} = {ivalue}) THEN {ivalue} '
            transformer_bin_dict[ifeature] = transformer_bin_dict[ifeature].replace(f'ELSE {ifeature} ', condition)

        temp = transformer_bin_dict[ifeature].split('END AS')
        transformer_bin_dict[ifeature] = temp[0] + 'ELSE -2 END AS' + temp[1]

        # Assign worst WOE to group -2
        woe_all = transformer_woe_dict[ifeature].replace(' THEN', ' WHEN').replace(' END AS', ' WHEN').split(' WHEN ')[2::2]
        woe_worst = max([float(i) for i in woe_all])
        temp = transformer_woe_dict[ifeature].split('END AS')
        transformer_woe_dict[ifeature] = temp[0] + f'WHEN ({ifeature} = -2) THEN {woe_worst} END AS' + temp[1]

    woe_transformer_major.transformer_bin_dict = transformer_bin_dict
    woe_transformer_major.transformer_woe_dict = transformer_woe_dict
    """
    ############################################## To remove in the retrained model Jan 2024###########################

    labelCol = 'label_30dpd'

    ftCols = [icol for icol in sdf_sample_loanstatus.columns if icol.startswith('ft_')]
    idCols = [icol for icol in sdf_sample_loanstatus.columns if icol not in [labelCol] + ftCols]
    excludeCols = idCols

    sdf_sample_loanstatus = sdf_sample_loanstatus.withColumn("label_30dpd", F.lit(0))

    sdf_loanstatus_woe = woe_transformer_major.transform(sdf_sample_loanstatus, labelCol, excludeCols)

    sdf_loanstatus_woe = sdf_loanstatus_woe.drop("label_30dpd")

    sdf_loanstatus_woe.printSchema()

    print("Step 2 Done: for WOE Encoding")

    # sdf_loanstatus_woe.limit(5).toPandas()

    # # Step 3: Generate logodds

    classification_model = GeneralizedLinearRegressionModel()
    model_loanhld_model = classification_model.load(
        model_parquet_path + f'{model_name}_{module}_{dpd}_{model_run_date}/model')

    vector_assembler = VectorAssembler()
    model_loanhld_assembler = vector_assembler.load(
        model_parquet_path + f'{model_name}_{module}_{dpd}_{model_run_date}/vector_assembler')

    vdf_loanstatus = model_loanhld_assembler.transform(sdf_loanstatus_woe)
    predictions_summary = model_loanhld_model.transform(vdf_loanstatus)
    predictions = predictions_summary.select('client_no', 'pt_date', 'prediction')

    predictions = predictions.withColumn('loanstatus_logodds', F.log(F.col('prediction') / (1 - F.col('prediction'))))

    predictions = predictions.withColumnRenamed("prediction", "pd")
    predictions.printSchema()

    print("Step 3 Done: for Generating Logodds")

    # # Step 4: Generate Outputs

    # Add "_woe" at the end of column names starting with "ft_"
    sdf_loanstatus_woe = sdf_loanstatus_woe.select(
        *[F.col(col_name).alias(col_name + "_woe") if col_name.startswith("ft_")
          else F.col(col_name) for col_name in sdf_loanstatus_woe.columns])

    columns_to_drop = ["mob", "seg_obs"]
    sdf_loanstatus_woe = sdf_loanstatus_woe.drop(*columns_to_drop)

    sdf_loanstatus_woe.printSchema()

    sdf = predictions.join(sdf_loanstatus_woe, on=["client_no", "pt_date"], how="left"). \
        join(sdf_sample_loanstatus, on=["client_no", "pt_date"], how="left")

    month_begin_datetime = datetime.datetime.strptime(feature_date, "%Y-%m-%d") + relativedelta(
        months=0) + relativedelta(day=1)
    month_begin = month_begin_datetime.strftime('%Y-%m-%d')

    month_end = (month_begin_datetime + relativedelta(months=1) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    sdf = sdf.withColumn("pt_date", F.lit(month_end))

    # for previous month end xdpd, We are not gonna score it therefore, assign null values to xdpd previous month end customers
    sdf = sdf.withColumn("loanstatus_logodds",
                         F.when(F.col("seg_obs") == "xdpd", None).otherwise(F.col("loanstatus_logodds")))

    sdf = sdf.withColumn("offline_score_date_loanstatus", F.lit(feature_date))
    sdf = sdf.withColumn("client_count", F.lit(sdf.count()))

    sdf = sdf.drop("label_30dpd")

    ############################# NEED TO UPDATE for the next model update ##########################################

    # to keep the consistent sequence of the feature in the dataframe for DWH write
    sdf = sdf.select("client_no",
                     "pd",
                     "loanstatus_logodds",
                     "ft_count_xdpd_sum_2m_woe",
                     "ft_count_xdpd_max_1m_woe",
                     "ft_disbursement_amount_max_2m_woe",
                     "ft_disbursement_count_max_3m_woe",
                     "ft_count_7dpd_sum_3m_woe",
                     "ft_pct_xdpd_change_3m_woe",
                     "ft_count_15dpd_sum_3m_woe",
                     "ft_count_7dpd_sum_1m_woe",
                     "ft_count_xdpd_change_3m_woe",
                     "ft_count_xdpd_change_ratio_1m_woe",
                     "ft_count_xdpd_sum_2m",
                     "ft_count_xdpd_max_1m",
                     "ft_disbursement_amount_max_2m",
                     "ft_disbursement_count_max_3m",
                     "ft_count_7dpd_sum_3m",
                     "ft_pct_xdpd_change_3m",
                     "ft_count_15dpd_sum_3m",
                     "ft_count_7dpd_sum_1m",
                     "ft_count_xdpd_change_3m",
                     "ft_count_xdpd_change_ratio_1m",
                     "offline_score_date_loanstatus",
                     "client_count",
                     "pt_date")

    feature_list = [col for col in sdf.columns if col.startswith("ft_")]
    fixed_cols = [col for col in sdf.columns if col not in feature_list]

    sdf = sdf.select(*fixed_cols,
                     F.to_json(F.struct(feature_list)).alias("loanstatus_features"))

    sdf.printSchema()

    # ## Write to Parquet

    # parquet_path_sample = "/production/"
    # sample_type = "loanstatus"

    """shelper.write_parquet(
            sdf = sdf,
            parquet_path = parquet_path_sample + f"sdf_production_{sample_type}_rundate={feature_date}"
        )"""

    print("Step 4 Done: for formating output")

    # ## Step 5: Write Parquet to DWH

    target_table = f'{db_prefix}dm.credit_risk_ph_ascore_loanstatus_offline_v2_ss_d'

    # clear the pt_date dwh table on current pt_date
    d = (datetime.datetime.strptime(dwh_date, "%Y-%m-%d") + relativedelta(months=-1) + relativedelta(day=1)).strftime(
        '%Y-%m-%d')
    # spark.sql(f"""-- alter table {target_table} drop if exists partition (pt_date = "{d}")""")
    spark.sql(f"""alter table {target_table} drop if exists partition (pt_date = "{month_end}")""")

    # insert the output to the dwh table
    sdf.write.mode('append').format('hive').partitionBy('pt_date').saveAsTable(target_table)

    print("Step 5 Done: DWH writing done")

    shelper.kill()

if __name__ == '__main__':
    loanstatus_module(dwh_date=sys.argv[1], run_environment=sys.argv[2], db_prefix=sys.argv[3])
