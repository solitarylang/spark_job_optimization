# -*- coding: utf-8 -*-
__author__ = 'Yao Chuhan'

import pyspark.sql.functions as func
from pyspark.sql import Window

from dao.device.device import Device
from dao.device.client_device import ClientDevice
from dao.cif_sample import CIFSample


class FTDevice:
    """
    pkey: client_no, device_id
    """

    def __init__(self, pkey='client_no', db_prefix: str=''):
        self.pkey = pkey
        self.db_prefix = db_prefix
        self.prefix = 'device'

        self.missing_values = [-999999, -999998, -999997]
        self.categorical_missing_values = ['NULL']
        self.categorical_features = ['device_last_os']

        self.period_ft_list = ['total_storage', 'used_storage', 'app_name', 'battery_level', 'battery_status']

        self.window_list = [30, 90, 180, 360]

    def get_ft_all(self, str_anchor):
        df_device = Device(db_prefix=self.db_prefix).get_raw_all(str_anchor)

        win = Window.partitionBy('device_id').orderBy(func.desc('collection_date'), 'num_nulls',
                                                      func.desc('collection_time'))
        df_last_device = df_device \
            .withColumn('collection_date', func.to_date('collection_time')) \
            .withColumn('num_nulls', sum(df_device[col].isNull().cast('int') for col in df_device.columns)) \
            .withColumn('rank', func.row_number().over(win)) \
            .filter(func.col('rank') == 1) \
            .drop('rank', 'collection_date', 'num_nulls')

        df_latest_price = Device(db_prefix=self.db_prefix).get_device_latest_price()

        df_ft_device_period = self.get_ft_device_period(str_anchor, df_device)
        df_ft_device_last = self.get_ft_device_last(str_anchor, df_last_device)
        df_ft_device_price = self.get_ft_device_price(str_anchor, df_last_device, df_latest_price)

        df_ft = df_ft_device_last.join(df_ft_device_period, on=self.pkey, how='left')
        df_ft = df_ft.join(df_ft_device_price, on=self.pkey, how='left')

        if self.pkey == 'client_no':
            df_ft_duration = self.get_ft_duration(str_anchor)
            df_ft_distinct_device_cnt = self.get_ft_distinct_device_cnt(str_anchor)

            df_client = df_ft_duration.join(df_ft_distinct_device_cnt, on=self.pkey, how='left')
            df_ft = df_client.join(df_ft, on=self.pkey, how='left')

        df_ft = self.add_prefix(df_ft, self.prefix, [self.pkey])
        df_ft = df_ft.fillna('NULL', subset=self.categorical_features)
        df_ft = df_ft.fillna(-999999, subset=list(set(df_ft.columns) - set(self.categorical_features)))

        if self.pkey == 'client_no':
            sample = CIFSample(db_prefix=self.db_prefix)
            df_sample = sample.get_raw(str_anchor).select(self.pkey)

            df_ft = df_sample.join(df_ft, on=self.pkey, how='left')
        return df_ft

    def get_ft_device_last(self, str_anchor, df_device):
        df_device = df_device.drop('model', 'brand', *self.period_ft_list)
        df_device = df_device \
            .withColumn('days_from_collection', func.datediff(func.to_date(func.lit(str_anchor)),
                                                              func.to_date('collection_time'))) \
            .drop('collection_time')

        if self.pkey == 'client_no':
            df_client = ClientDevice(db_prefix=self.db_prefix).get_last_device(str_anchor)
            df_device = df_client.join(df_device, on='device_id', how='left').drop('device_id')

        df_ft = self.add_prefix(df_device, 'last', [self.pkey])
        return df_ft

    def get_ft_device_period(self, str_anchor, df_device):
        df_device = df_device.select('device_id', *self.period_ft_list, 'network_is_wifi')
        if self.pkey == 'client_no':
            df_client = ClientDevice(db_prefix=self.db_prefix).get_last_device(str_anchor)
            df_device = df_client.join(df_device, on='device_id', how='left').drop('device_id')

        df_ft = df_device.withColumn('used_storage_ratio', func.col('used_storage') / func.col('total_storage'))

        lst_expr = [
            func.avg('used_storage').alias('avg_used_storage'),
            func.avg('total_storage').alias('avg_total_storage'),
            func.avg('used_storage_ratio').alias('avg_used_storage_ratio'),
            func.avg(
                func.when(func.col('battery_level') >= 90, 1).when(func.col('battery_level').isNotNull(), 0)).alias(
                'battery_level_ge_90_ratio'),
            func.avg(func.when(func.col('battery_status').isin(['charging', 'fully_charged']), 1).when(
                func.col('battery_status').isNotNull(), 0)).alias('battery_charging_ratio'),
            func.avg('network_is_wifi').alias('network_wifi_ratio'),
            func.avg(func.when(func.col('app_name') == 'seabank', 1).when(func.col('app_name').isNotNull(), 0)).alias(
                'trigger_app_seabank_ratio'),
            func.avg(func.when(func.col('app_name') == 'shopee', 1).when(func.col('app_name').isNotNull(), 0)).alias(
                'trigger_app_shopee_ratio')
        ]

        df_ft = df_ft.groupBy(self.pkey).agg(*lst_expr)
        if self.pkey == 'client_no':
            df_ft = self.add_prefix(df_ft, 'last', [self.pkey])
        return df_ft

    def get_ft_duration(self, str_anchor):
        df_client = ClientDevice(db_prefix=self.db_prefix).get_raw(str_anchor)
        df_last = ClientDevice(db_prefix=self.db_prefix).get_last_device(str_anchor) \
            .withColumnRenamed('days_from_operation', 'days_from_last_operation') \
            .drop('device_id')
        df_client = df_client.join(df_last, on=self.pkey, how='left')

        df_ft = df_client.groupBy(self.pkey) \
            .agg(func.max(func.col('days_from_operation') - func.col('days_from_last_operation'))
                 .alias(f'operation_duration_{max(self.window_list)}d'))
        return df_ft

    def get_ft_distinct_device_cnt(self, str_anchor):
        df_client = ClientDevice(db_prefix=self.db_prefix).get_raw(str_anchor)

        lst_expr_cnt = []
        group_expr = []
        lst_expr = []

        for w in self.window_list:
            lst_expr_cnt.append(
                func.sum(func.when(func.col('days_from_operation') < w, 1).otherwise(0)).alias(f'cnt_{w}d'))

            group_expr.append(
                func.max(func.when(func.col('days_from_operation') < w, 1).otherwise(0)).alias(f'is_{w}d'))
            lst_expr.append(func.sum(f'is_{w}d').alias(f'distinct_device_cnt_{w}d'))

        df_ft_cnt = df_client.groupBy(self.pkey).agg(*lst_expr_cnt)
        df_ft = df_client \
            .groupBy(self.pkey, 'device_id') \
            .agg(*group_expr) \
            .groupBy(self.pkey) \
            .agg(*lst_expr)

        df_ft = df_ft_cnt.join(df_ft, on=self.pkey, how='left')

        for w in self.window_list:
            df_ft = df_ft.withColumn(f'distinct_device_cnt_norm_{w}d',
                                     func.when(func.col(f'cnt_{w}d') == 0, -999998).otherwise(
                                         func.col(f'distinct_device_cnt_{w}d') / func.col(f'cnt_{w}d')))
            df_ft = df_ft.drop(f'cnt_{w}d')
        return df_ft

    def get_ft_device_price(self, str_anchor, df_device, df_latest_price):
        df_device = df_device.select('device_id', 'model', 'brand')
        df_latest_price = df_latest_price.select('model', 'brand', 'price_usd_median').withColumnRenamed(
            'price_usd_median', 'last_price_usd')

        if self.pkey == 'client_no':
            df_client = ClientDevice(db_prefix=self.db_prefix).get_last_device(str_anchor).select(self.pkey, 'device_id')
            df_device = df_client.join(df_device, on='device_id', how='left').drop('device_id')

        df_ft = df_device.join(df_latest_price, on=['model', 'brand'], how='left').drop('model', 'brand')
        return df_ft

    @staticmethod
    def add_prefix(df_ft, prefix, pkey):
        lst_col = sorted(set(df_ft.columns) - set(pkey))
        lst_col = [func.col(x).alias(f'{prefix}_{x}') for x in lst_col]
        df_ft = df_ft.select(*pkey, *lst_col)
        return df_ft


def main():
    pass


if __name__ == '__main__':
    main()
