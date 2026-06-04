# -*- coding: utf-8 -*-
__author__ = "Chengxi Xue"

import datetime
import sys

from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as func
import pyspark.sql.types as tp
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

class DWDApps:
    def __init__(self,db_prefix:str=''):
        self.table = {'device': f'{db_prefix}ods.mbs_dispatch_center_message_log_hi',
                      'antifraud':f'{db_prefix}ods.mbs_app_anti_fraud_ss',
                      # 'lending_apps_table':'',
                      # 'high_risk_apps_table':'',
                      # 'app_black_list':''
        }

        self.lending_app_list = {
        'com.jk.opeso.plus',
        'ph.lhl.pautangpeso',
        'com.perabag.app',
        'ph.cashmore.loan',
        'ooifxdso.opxosduf.oqexred',
        'tech.codeblock.okpeso',
        'com.diyi.snapera',
        'ph.robocash.mobile',
        'ph.loans.mobile',
        'com.peranyo.cash.personal.loan.credit.peso.fast.lend.easy.quick.borrow.online.ph',
        'com.PondoCash.www',
        'u.money.spend',
        'ph.zippeso.online.cash.loan',
        'com.lanhai.upesocash',
        'com.ncvi.cebxpress',
        'com.tmart.pesoq',
        'com.shuiyiwenhua.gl',
        'com.pesokuwento.ph',
        'com.taga.cash',
        'ph.fast.lending',
        'com.pesohere.phk',
        'com.juanhand.fast.cash.peso.loan.app',
        'com.peramoo',
        'com.linkcredited.ipeso',
        'com.yyjg.gp',
        'com.wow.pera',
        'com.honey.loan',
        'com.clearmindai.ussc.panalowallet',
        'com.robofinance.unacash',
        'com.SurityCash.hxov',
        'com.happyperatwo.app',
        'com.u5.e_perash',
        'com.myloan.pesocash',
        'com.plentina.app',
        'com.pesoloan',
        'com.andsystems.lendpinoy',
        'loanchamp.com.loanchamp',
        'com.spendcash.cash.ph',
        'org.cashbee.quasar.app',
        'com.hpera.app',
        'hb.xinxinxr.vip',
        'hb.xinxin.xiangrong',
        'com.linkcredited.pesoin',
        'ph.moneycat',
        'com.suncash.loan',
        'pppi.earth.pinoylend',
        'com.peso24h.philippine',
        'www.swip.pera',
        'ph.cashme666.android',
        'com.vpeso.borrow.loan.app',
        'com.finstar.loan.app',
        'com.qzflb.bigloan',
        'com.loan.yopeso.app.yopeso',
        'ph.onlineloans.mobile.android',
        'com.fincredit.sdthrsh',
        'tech.codeblock.kpeso',
        'com.abs.pesobuffet.android',
        'com.flb.cashbox',
        'com.pesoonline',
        'com.mocamoca',
        'com.fclc.cashcowdwd'
        }

    def get_raw(self, str_anchor, offset=360):
        str_anchor = min((datetime.date.today() - relativedelta(days=1)).strftime('%Y-%m-%d'), str_anchor)
        start_date = max('2024-03-07', (datetime.date.today() - relativedelta(days=offset)).strftime('%Y-%m-%d'))
        # 1.order by kafka_ts as within same pt_date, it could have different records.
        # 2.keep the rank here as this function will consider all apps installed, and now in ph, one time sampling can only get maximam 100 apps

        sql = f"""
            WITH device_tab AS
            (
                SELECT 
                    get_json_object(message, '$.deviceId') AS device_id,
                    get_json_object(message, '$.deviceInfo.os') AS os,
                    get_json_object(message,'$.deviceInfo.apps') AS apps,
                    DATEDIFF(DATE('{str_anchor}'), DATE(pt_date)) AS diff_days,
                    ROW_NUMBER() OVER(PARTITION BY get_json_object(message, '$.deviceId') ORDER BY kafka_ts DESC) AS rank,
                    pt_date
                FROM {self.table['device']}
                WHERE pt_date <= '{str_anchor}'
                    AND pt_date >= '{start_date}'
                    AND pt_biz = 'device-security'
                    AND get_json_object(message, '$.deviceId') IS NOT NULL
                    AND get_json_object(message, '$.deviceInfo.os') IS NOT NULL
                    AND get_json_object(message, '$.deviceInfo.apps') IS NOT NULL
            )
            SELECT 
                device_id,
                apps,
                os,
                diff_days,
                rank,
                pt_date
            FROM device_tab
            """

        df_raw = SparkSession.builder.getOrCreate().sql(sql)

        return df_raw

    def get_apps_tag(self, str_anchor):

        df_raw = self.get_raw(str_anchor)
        df_device_apps = df_raw.withColumn('app_info_list', func.from_json('apps', tp.ArrayType(tp.StringType())))
        df_device_apps = df_device_apps['device_id', 'apps', 'app_info_list', 'pt_date','rank']
        df_device_apps = df_device_apps.select(
            "device_id",
            func.explode("app_info_list").alias("app_id"),
            # This explodes 'APP_lists' into separate rows and names the new column 'APP'
            "pt_date",
            "rank"
        )

        column_helper1 = [func.when(
            func.size(func.split(func.col("app_id"), ",")) >= 2,
            func.split(func.col("app_id"), ",").getItem(1)
        ).otherwise(None).alias("app_id") if col_name == "app_id"
                       else col_name for col_name in df_device_apps.columns]

        column_helper2 = [
            # For the 'app_info' column, apply the decoder logic
            func.when(
                func.col("app_id").startswith('-'),
                func.concat(func.lit("com."), func.expr("substring(app_id, 2)"))
            ).otherwise(func.col("app_id")).alias("app_id") if column == "app_id"
            else column for column in df_device_apps.columns
        ]

        df_device_apps = df_device_apps.select(*column_helper1)
        df_device_apps = df_device_apps.select(*column_helper2)
        df_device_apps = df_device_apps.select("device_id", "app_id", "pt_date")
        df_device_apps = df_device_apps.dropDuplicates()
        #  device_id              app_info                pt_date
        # 0 1727580000003779554 com.globe.gcash.android 2024 - 03 - 13
        # 1 1727580000003779554 com.magic.solitairegame 2024 - 03 - 13

        # add risk tag, how to get the lending app list can be changed later.
        data = [(item,) for item in self.lending_app_list]

        spark = SparkSession.builder.getOrCreate()
        df_risk_list = spark.createDataFrame(data, ['lending_app_id'])

        df_device_apps = df_device_apps.join(df_risk_list, df_device_apps.app_id == df_risk_list.lending_app_id, 'left')
        df_device_apps = df_device_apps.withColumn('risk_tag',
                                                   func.when(func.col('lending_app_id').isNull(), 0).otherwise(1))
        df_device_apps = df_device_apps.drop('lending_app_id')
        # device_id              app_id                  pt_date        risk_tag
        # 0 1727580000006436615 ph.com.singlife.app     2024 - 03 - 13 0
        # 1 1727580000010382556 com.instagram.barcelona 2024 - 03 - 13 0

        return df_device_apps

    def get_dwd(self, str_anchor, offset=720, dpd=30):
        pt_date = (datetime.datetime.today() - relativedelta(days=1)).strftime("%Y-%m-%d")
        str_anchor = min(str_anchor, pt_date)

        df_device_apps = self.get_apps_tag(str_anchor)

        #device_id              app_id                  pt_date        risk_tag
        # 0 1727580000006436615 ph.com.singlife.app     2024 - 03 - 13 0
        # 1 1727580000010382556 com.instagram.barcelona 2024 - 03 - 13 0

        # get app list on the latest pt_date
        windowSpec = Window.partitionBy('device_id')
        latest_dates = df_device_apps.withColumn('latest_pt_date', func.max('pt_date').over(windowSpec))
        # check whether one pt_date will have duplicated records
        latest_apps = latest_dates.filter(latest_dates['pt_date'] == latest_dates['latest_pt_date'])
        latest_apps = latest_apps.dropDuplicates(['device_id', 'app_id'])

        # get app list for all history pt_date
        diff_day = datetime.datetime.strptime(str_anchor, '%Y-%m-%d') - datetime.timedelta(days=offset)

        recent_apps = df_device_apps.filter(func.col('pt_date') >= func.lit(diff_day))
        recent_apps = recent_apps.dropDuplicates(['device_id', 'app_id'])
        # here no need to check pt_date as we want to include all apps

        # Feature 1
        # generate first feature - app_latest_lending_cnt
        # Number of lending apps installed on the given device id at the last time of data collection.
        combined_apps_flag = latest_apps.groupBy('device_id').agg(func.sum('risk_tag').alias('app_latest_lending_cnt'))

        # Feature 2
        # generate second feature - app_historical_lending_cnt
        # number of different lending apps installed on given device id over past 2 years
        df_lending_app_hist = recent_apps.groupBy('device_id').agg(func.sum('risk_tag').alias('app_historical_lending_cnt'))

        # combine together
        df_dwd = combined_apps_flag.join(df_lending_app_hist, on='device_id', how='left')

        # missing value can be 0, but it is only for policy. if for features, it should not be 0. as 0 means good users, rather than no value

        df_dwd = df_dwd.fillna(0, subset=['app_latest_lending_cnt', 'app_historical_lending_cnt'])

        # output
        # device_id app_latest_lending_cnt app_historical_lending_cnt'
        # 0 xxxxxx 0 0
        # 1 xxxxx 0 0


        return df_dwd

def main(dt: str,db_prefix:str):
    job_name = "ads_udl_customer_underwriting_feature_app_df"
    spark = SparkSession.builder.appName(job_name) \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport().getOrCreate()

    df_dwd = DWDApps(db_prefix=db_prefix).get_dwd(dt)
    path_name = f'/user/hive/warehouse/{db_prefix}bmart_creditrisk.db/ads_udl_customer_underwriting_feature_app_df/pt_date={dt}'
    df_dwd.repartition(10).write.orc(path_name, mode='overwrite')  # dwh修改


if __name__ == '__main__':
    dt = sys.argv[1]
    db_prefix = sys.argv[2]
    main(dt, db_prefix)

