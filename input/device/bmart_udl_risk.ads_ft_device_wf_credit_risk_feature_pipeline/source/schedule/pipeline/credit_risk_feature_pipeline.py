# -*- coding: utf-8 -*-

import sys
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def run_feature_pipeline(pt_date, table_name, ft_class):
    df_ft = ft_class.get_ft_all(pt_date)  # feature table use get_ft_all
    df_ft = df_ft.withColumn("pt_date", lit(pt_date))
    df_ft.printSchema()

    spark = SparkSession.builder.getOrCreate()
    try:
        describe_df = spark.sql(f"DESCRIBE {table_name}")
        describe_rows = [row.asDict() for row in describe_df.collect()]

        data_cols = []
        in_partition_section = False

        for row in describe_rows:
            col_name = row["col_name"].strip()
            data_type = row["data_type"] or ""

            if col_name == "# Partition Information":
                in_partition_section = True
                continue

            if not in_partition_section:
                if data_type:
                    data_cols.append(col_name)

        target_columns = data_cols

        print(f"目标表字段解析完成 → 数据列[{len(data_cols)}]")

        missing_cols = [col for col in target_columns if col not in df_ft.columns]
        if missing_cols:
            print(f"数据源缺失以下字段: {', '.join(missing_cols)}")
        print(target_columns)
        df_ordered = df_ft.select(target_columns)

        df_ordered.write \
            .format("hive") \
            .mode("overwrite") \
            .insertInto(table_name)
        print(f"数据成功写入 {table_name} 分区(pt_date={dt})")

    except AnalysisException as e:
        if 'table or view not found' in str(e).lower():
            df_ft.write \
                .format("hive") \
                .partitionBy("pt_date") \
                .mode("overwrite") \
                .saveAsTable(table_name)
            print(f"新建分区表 {table_name} 并写入数据成功")
        else:
            raise e
    finally:
        spark.stop()
    # spark.sql("ALTER TABLE {table_name} DROP IF EXISTS PARTITION  (pt_date='{dt}')".format(table_name=table_name, dt=pt_date))


def main(dt, ft_name, table_name, db_prefix: str = ''):
    from features.app.ft_app import FTAPP
    from features.device.ft_device import FTDevice
    from features.loan.udl.ft_bill import FTBill
    from features.loan.udl.ft_limit import FTLimit
    from features.loan.udl.ft_loan import FTLoan
    from features.loan.udl.ft_repayment import FTRepayment
    from features.user_graph.ft_device_graph import FTDeviceGraph
    from features.user_graph.ft_phonebook_graph import FTPhonebookGraph
    from features.gps.hex.ft_gps import FTGPS
    from features.cic.ft_cic_crif_info import FTCICCrifInfo
    from features.cic.ft_cic_demogr import FTCICDemogr
    from features.cic.ft_cic_loan_all import FTCICLoanAll
    from features.cic.ft_cic_query_all import FTCICQueryAll
    from features.loan.ft_loan import FTCHNLLoan

    JOB_NAME = f"{table_name}_credit_risk_feature_pipeline.py"

    spark = SparkSession.builder.appName(JOB_NAME) \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport().getOrCreate()

    dict_ft = {
        'ft_device_user_level': FTDevice(pkey='client_no', db_prefix=db_prefix),
        'ft_app_user_level': FTAPP(pkey='client_no', db_prefix=db_prefix),

        'ft_udl_limit': FTLimit(db_prefix=db_prefix),
        'ft_udl_loan': FTLoan(db_prefix=db_prefix),
        'ft_udl_bill': FTBill(db_prefix=db_prefix),
        'ft_udl_repay': FTRepayment(db_prefix=db_prefix),

        'ft_phonebook': FTPhonebookGraph(db_prefix=db_prefix),
        'ft_rev_phonebook': FTPhonebookGraph(direction='reverse', db_prefix=db_prefix),

        'ft_device': FTDevice(pkey='device_id', db_prefix=db_prefix),
        'ft_app_device_level': FTAPP(pkey='device_id', db_prefix=db_prefix),

        'ft_device_graph': FTDeviceGraph(pkey='device_id', db_prefix=db_prefix),
        'ft_device_graph_user_level': FTDeviceGraph(pkey='client_no', db_prefix=db_prefix),
        'ft_phonebook_both': FTPhonebookGraph(direction='both', db_prefix=db_prefix),

        'gps_hex_level': FTGPS(db_prefix=db_prefix),

        'ft_cic_crif_info':FTCICCrifInfo(db_prefix=db_prefix),
        'ft_cic_demogr':FTCICDemogr(db_prefix=db_prefix),
        'ft_cic_loan_all':FTCICLoanAll(db_prefix=db_prefix),
        'ft_cic_query_all':FTCICQueryAll(db_prefix=db_prefix),

        'ads_ft_chnl_loan_v2_df':FTCHNLLoan(db_prefix=db_prefix),
    }

    pt_date = dt
    ft_class = dict_ft[ft_name]
    print(f"str_anchor is {pt_date}, table name is {table_name}")
    print(f'start processing {ft_name} {pt_date} ... ')
    run_feature_pipeline(pt_date, table_name, ft_class)


if __name__ == '__main__':
    dt = sys.argv[1]
    ft_name = sys.argv[2]
    table_name = sys.argv[3] + '.' + sys.argv[4]
    db_prefix = sys.argv[5]
    print(f"ft_name:{ft_name}, table_name:{table_name},db_prefix:{db_prefix}")
    main(dt, ft_name, table_name, db_prefix)
