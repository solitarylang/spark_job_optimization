# -*- coding: utf-8 -*-
__author__ = 'Yao Chuhan'

from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql import Window


class ClientDevice:
    def __init__(self, db_prefix: str = ''):
        self.table = {'antifraud': f'{db_prefix}fmart_antifraud.dwd_antifraud_action_log_di'}

    def get_raw(self, str_anchor, offset=360):
        sql = f"""
            WITH device_tab AS (
                SELECT 
                    client_no AS client_no,
                    device_id AS device_id,
                    DATEDIFF(DATE('{str_anchor}'), FROM_UNIXTIME(action_timestamp / 1000)) AS days_from_operation
                FROM {self.table['antifraud']}
                WHERE device_id IS NOT NULL and device_id != ''
                    AND client_no IS NOT NULL and client_no != ''
                    AND scene_code IN (1001, 1061, 1072, 1073, 1074, 1091, 1092, 1145, 1151, 1153, 1156)
                    -- Login / DLOnboarding / DLDrawdown
                    AND DATEDIFF(DATE('{str_anchor}'), FROM_UNIXTIME(action_timestamp / 1000)) >= 0
                    AND DATEDIFF(DATE('{str_anchor}'), FROM_UNIXTIME(action_timestamp / 1000)) < {offset}
                    AND action_status = '1' -- successful operation
            )
            SELECT 
                client_no,
                device_id,
                days_from_operation
            FROM device_tab
            GROUP BY client_no, device_id, days_from_operation
        """

        df_raw = SparkSession.builder.getOrCreate().sql(sql)
        return df_raw


    def get_last_device(self, str_anchor):
        df_device = self.get_raw(str_anchor)

        win = Window.partitionBy('client_no').orderBy('days_from_operation', 'device_id')
        df_device_last_record = df_device \
            .withColumn('most_recent_device_rank', func.row_number().over(win)) \
            .filter(func.col('most_recent_device_rank') == 1) \
            .drop('most_recent_device_rank')

        return df_device_last_record

    def get_common_device(self, str_anchor):
        df_device = self.get_raw(str_anchor)
        df_device = df_device.groupBy(['client_no', 'device_id']).count()

        win = Window.partitionBy('client_no').orderBy(func.desc('count'), 'device_id')
        df_device_common_record = df_device \
            .withColumn('most_common_device_rank', func.row_number().over(win)) \
            .filter(func.col('most_common_device_rank') == 1) \
            .drop('most_common_device_rank', 'count')

        return df_device_common_record


def main():
    pass


if __name__ == '__main__':
    main()
