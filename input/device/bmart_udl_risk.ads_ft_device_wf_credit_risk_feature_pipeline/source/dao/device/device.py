# -*- coding: utf-8 -*-
__author__ = 'Yao Chuhan'

from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql import Window

from dao.device.config import Config


class Device:
    def __init__(self, db_prefix: str = ''):
        self.table = {'device_middle_table': f'{db_prefix}bmart_udl_risk.dwd_device_di'}
        self.db_prefix = db_prefix
        self.pkey = ['device_id', 'collection_time']
        self.exclude = ['device_id', 'os', 'model', 'app_name', 'collection_time']

        self.missing_values = [-999997]

    def get_raw(self, str_anchor, offset=360):

        sql = f"""
            SELECT 
                device_id,
                os,
                os_ver,
                model,
                app_name,
                is_jailbreak,
                android_app_ver,
                android_sim_card,
                android_vpn,
                android_boot_time,
                android_internal_storage,
                android_carrier,
                android_network_type,
                android_battery_level,
                android_battery_status,
                android_screen,
                android_os_ver_int,
                android_brand,
                android_manufacturer,
                android_cpu_count,
                android_is_emulator,
                ios_app_ver,
                ios_is_sim_card,
                ios_is_vpn_connected,
                ios_resume_boot_time,
                ios_free_storage,
                ios_total_storage,
                ios_carrier,
                ios_network_type,
                ios_battery,
                ios_screen,
                ios_memory,
                collection_time
            FROM {self.table['device_middle_table']}
            WHERE pt_date <= '{str_anchor}'
                AND DATE(pt_date) > DATE('{str_anchor}') - INTERVAL '{offset}' DAY
                AND DATE(collection_time) <= DATE('{str_anchor}')
        """

        df_device = SparkSession.builder.getOrCreate().sql(sql)
        df_device = df_device.dropDuplicates(subset=['device_id', 'collection_time'])
        return df_device

    def get_raw_all(self, str_anchor):
        df_raw = self.get_raw(str_anchor)

        df_android = self.get_raw_android(str_anchor, df_raw)
        df_ios = self.get_raw_ios(str_anchor, df_raw)

        for c in list(set(df_ios.columns) - set(df_android.columns)):
            df_android = df_android.withColumn(c, func.lit(-999997))
        for c in list(set(df_android.columns) - set(df_ios.columns)):
            if c == 'brand':
                df_ios = df_ios.withColumn(c, func.lit('apple'))
            else:
                df_ios = df_ios.withColumn(c, func.lit(-999997))
        df_android = df_android.select(*df_ios.columns)

        df_all = df_android.union(df_ios)

        # process app name & version
        df_all = df_all.withColumn('app_name',
                                   func.when(func.col('app_name').isin(Config.dict_app_name['seabank']), 'seabank')
                                   .when(func.col('app_name').isin(Config.dict_app_name['shopee']), 'shopee'))

        lst_expr = []
        for c, l in Config.dict_app_name.items():
            lst_expr.extend([
                func.when(func.col('app_name') == c,
                          func.split('app_ver', '\.').getItem(0).cast('float') + func.split('app_ver', '\.')
                          .getItem(1).cast('float') / 100).alias(f'{c}_app_ver')])
        df_all = df_all.select(*df_all.columns, *lst_expr).drop('app_ver')
        return df_all

    def get_raw_ios(self, str_anchor, df_raw=None):
        if df_raw is None:
            df_raw = self.get_raw(str_anchor)
        df_ios = df_raw.filter(func.col('os').isin(['iOS', 'iPadOS']))

        lst_expr = [
            func.split('os_ver', '\.').getItem(0).cast('int').alias('ios_os_ver'),
            (func.size(func.split(func.col('is_jailbreak'), '1')) - 1).alias('is_jailbreak'),
            func.col('ios_app_ver').alias('app_ver'),
            func.col('ios_is_sim_card').cast('int').alias('is_sim_card'),
            func.col('ios_is_vpn_connected').cast('int').alias('is_vpn_connected'),
            func.datediff(func.lit(str_anchor),
                          func.from_unixtime(func.split('ios_resume_boot_time', '\|').getItem(1))).alias(
                'days_from_boot'),
            (func.col('ios_total_storage').cast('float') / 1073741824.0).alias('total_storage'),
            (func.when(func.col('ios_free_storage') <= func.col('ios_total_storage'),
                       func.col('ios_total_storage') - func.col('ios_free_storage')).cast(
                'float') / 1073741824.0).alias('used_storage'),
            func.lower('ios_carrier').alias('carrier'),
            func.lower('ios_network_type').alias('network_type'),
            func.split('ios_battery', '\%\|').getItem(0).cast('int').alias('battery_level'),
            func.split('ios_battery', '\%\|').getItem(1).cast('int').alias('battery_status'),
            func.split('ios_screen', ',').getItem(0).cast('int').alias('screen_width'),
            (func.col('ios_memory').cast('float') / 1073741824.0).alias('ios_memory')
        ]

        df_ios = df_ios.select(*self.exclude, *lst_expr)

        df_ios = df_ios \
            .withColumn('battery_status', func.when(func.col('battery_status') == 1, 'not_charging')
                        .when(func.col('battery_status') == 2, 'charging')
                        .when(func.col('battery_status') == 3, 'fully_charged')) \
            .withColumn('is_jailbreak', func.when(func.col('is_jailbreak') >= 2, 1).otherwise(0))

        lst_expr = []
        # process network type
        for c, l in Config.dict_network.items():
            lst_expr.extend(
                [func.when(func.col('network_type').isin(l), 1).when(func.col('network_type').isNotNull(), 0).alias(c)])

        # process carrier
        for c, l in Config.dict_carrier.items():
            lst_expr.extend([func.when(func.col('carrier').isin(l), 1).otherwise(0).alias(c)])

        df_ios = df_ios.select(*df_ios.columns, *lst_expr).drop('carrier', 'network_type')
        df_ios = df_ios.fillna(0, subset=['is_sim_card', 'is_vpn_connected'])
        return df_ios

    def get_raw_android(self, str_anchor, df_raw=None):
        if df_raw is None:
            df_raw = self.get_raw(str_anchor)
        df_android = df_raw.filter(func.col('os') == 'android')

        lst_expr = [
            func.col('android_os_ver_int').cast('int').alias('android_os_ver'),
            'is_jailbreak',
            func.col('android_app_ver').alias('app_ver'),
            func.split('android_sim_card', ',').alias('is_sim_card'),
            func.when(func.col('android_vpn').isNull(), 0).otherwise(1).alias('is_vpn_connected'),
            func.datediff(func.lit(str_anchor), func.from_unixtime(func.col('android_boot_time') / 1000)).alias(
                'days_from_boot'),
            (func.split(func.split('android_internal_storage', ',').getItem(2), 'B').getItem(0).cast(
                'float') / 1073741824.0).alias('total_storage'),
            (func.split(func.split('android_internal_storage', ',').getItem(3), 'B').getItem(0).cast(
                'float') / 1073741824.0).alias('used_storage'),
            func.split('android_carrier', ',').alias('carrier'),
            func.split('android_network_type', ',').alias('network_type'),
            func.col('android_battery_level').cast('int').alias('battery_level'),
            func.col('android_battery_status').cast('int').alias('battery_status'),
            func.split('android_screen', ',').getItem(0).cast('int').alias('screen_width'),
            func.lower('android_brand').alias('brand'),
            func.lower('android_manufacturer').alias('manufacturer'),
            func.col('android_cpu_count').cast('int').alias('android_cpu_count'),
            'android_is_emulator'
        ]

        df_android = df_android.select(*self.exclude, *lst_expr)

        df_android = df_android \
            .withColumn('used_storage',
                        func.when(func.col('used_storage') <= func.col('total_storage'), func.col('used_storage'))) \
            .withColumn('battery_status', func.when(func.col('battery_status') == 2, 'charging')
                        .when(func.col('battery_status').isin([3, 4]), 'not_charging')
                        .when(func.col('battery_status') == 5, 'fully_charged')) \
            .withColumn('is_jailbreak', func.when(func.col('is_jailbreak') == 'true', 1)
                        .when(func.col('is_jailbreak') == 'false', 0)) \
            .withColumn('android_is_emulator', func.when(func.col('android_is_emulator') == 'true', 1)
                        .when(func.col('android_is_emulator') == 'false', 0))

        # process is_sim_card
        df_sim_card = df_android.select(*self.pkey, func.explode_outer('is_sim_card').alias('sim_card'))
        df_sim_card = df_sim_card.withColumn('is_sim_card',
                                             func.when(func.col('sim_card').isin(['LOADED', 'READY']), 1).otherwise(0))
        df_sim_card = df_sim_card.groupBy(self.pkey).agg(func.max('is_sim_card').alias('is_sim_card'))

        df_android = df_android.drop('is_sim_card').join(df_sim_card, on=self.pkey, how='left')

        # process network type
        df_network = df_android.select(*self.pkey, func.explode_outer('network_type').alias('network'))
        df_network = df_network.withColumn('network', func.lower('network'))

        lst_expr = []
        group_expr = []
        for c, l in Config.dict_network.items():
            lst_expr.extend(
                [func.when(func.col('network').isin(l), 1).when(func.col('network').isNotNull(), 0).alias(c)])
            group_expr.extend([func.max(func.col(c)).alias(c)])

        df_network = df_network.select(*self.pkey, *lst_expr).groupBy(self.pkey).agg(*group_expr)
        df_android = df_android.join(df_network, on=self.pkey, how='left')

        # process carrier
        df_carrier = df_android.select(*self.pkey, func.explode_outer('carrier').alias('carrier'))
        df_carrier = df_carrier.withColumn('carrier', func.lower('carrier'))

        lst_expr = []
        group_expr = []
        for c, l in Config.dict_carrier.items():
            lst_expr.extend([func.when(func.col('carrier').isin(l), 1).otherwise(0).alias(c)])
            group_expr.extend([func.max(func.col(c)).alias(c)])

        df_carrier = df_carrier.select(*self.pkey, *lst_expr).groupBy(self.pkey).agg(*group_expr)
        df_android = df_android.join(df_carrier, on=self.pkey, how='left')

        # process manufacturer
        lst_expr = []
        for c, b in Config.dict_manufacturer.items():
            lst_expr.extend([func.when(func.col('manufacturer') == b, 1).otherwise(0).alias(c)])

        df_android = df_android.select(*df_android.columns, *lst_expr).drop('carrier', 'network_type', 'manufacturer')
        df_android = df_android.fillna(0, subset=['is_jailbreak'])
        return df_android

    def get_device_latest_price(self):
        if self.db_prefix == '':
            path = Config.PRICE_PATH
        else:
            path = Config.PRICE_PATH_NPT
        df_price = SparkSession.builder.getOrCreate().read.load(path, format='csv', sep=',',
                                                                inferSchema='true', header='true')
        df_price = df_price.withColumn('grass_date', func.to_date('grass_date', 'd/M/yy'))

        win = Window.partitionBy('model', 'brand').orderBy(func.desc('grass_date'))
        df_price = df_price \
            .withColumn('rank', func.row_number().over(win)) \
            .filter(func.col('rank') == 1) \
            .drop('rank')
        return df_price


def main():
    pass


if __name__ == '__main__':
    main()
