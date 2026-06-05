"""
spark util function for id bank environment
"""

# -*- coding: utf-8 -*-
__version__ = "v1.0"
__authors__ = {"Dan Chan", "Jia Jun Zhang"}

import sys, os
import datetime

# import findspark
# findspark.init('/usr/share/spark', edit_rc=True)

import pyspark
from pyspark.sql import SQLContext, HiveContext, SparkSession
from pyspark import SparkContext, SparkConf

import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType, FloatType, StringType, BooleanType, DateType


class PHBankSpark():
    """
    helper class for spark
    """
    
    # default config dictionary
    spark_config_default = {
        'spark.executor.heartbeatInterval': '100s',
        'spark.sql.broadcastTimeout': 20 * 600,
        'spark.network.timeout': 5 * 60 * 1000,
        'spark.locality.wait': '30000ms',
        'spark.sql.hive.manageFilesourcePartitions': 'true',
        'spark.shuffle.consolidateFiles': 'true',
        'spark.executor.cores': 4,
        'spark.executor.memory': '16g',
        'spark.executor.memoryOverhead': '2g',
        'spark.executor.instances': 16,
        'spark.debug.maxToStringFields': 500,
        'spark.speculation': 'false',
        'spark.ui.showConsoleProgress': 'false',
        'spark.memory.fraction': 0.55,
        'spark.driver.memory': '20g',
        'spark.scheduler.listenerbus.eventqueue.capacity': 20000,
        'spark.kryoserializer.buffer.max': '2047m',
        'spark.sql.execution.arrow.pyspark.enabled': False,
        'spark.driver.allowMultipleContexts': True,
        'spark.yarn.queue': 'dsclient',
        'spark.dynamicAllocation.enabled': True,
        # Added 2023-05-22 due to new queue created for reg risk users
        'spark.yarn.dist.archives': 'hdfs:///user/bi/regriskds/dist/conda_env_base_2023-09.tar.gz',
        # 'spark.submit.deployMode' : 'cluster', # parameter not present in shopee and throws an error when used
    }
    
    def __init__(self, **kwargs):
        """
        :param app_name: str() of app name
        :param config_dict: dict() of spark config 
            (e.g. self.spark_config_default)
        :return: None
        """
        # default app_name
        time_now = datetime.datetime.now().strftime("%Y-%m-%d_%H%Mh")
        def_app_name = f"reg_risk_{time_now}"
        
        # set app name
        self.app_name = kwargs.get("app_name", def_app_name)
        
        # set python path(From shopee)
        # os.environ["SPARK_HOME"] = "usr/share/spark-3.1"
        # os.environ["SPARK_CONF_DIR"] = "usr/spark-3.1"
        os.environ["PYSPARK_PYTHON"] = "/bin/python3"
        # os.environ["PYTHONPATH"] = "/usr/share/spark3/python:$PYTHONPATH"
        
        # spark default config updated with specified config_dict
        self.config_dict = self.spark_config_default.copy()
        self.config_dict.update(kwargs.get("config_dict", dict()))
        
        # init session as None
        self.session = None
        
        return None
        
    def build_session(self, **kwargs):
        """
        function to build spark session with object parameters
        note: run self.kill() to stop session
        
        :return: spark session object
        """
        
        # init spark config object
        self.spark_conf = SparkConf()
        # load spark configuraton settings
        for k, v in self.config_dict.items():
            # spark_conf.set(k, v)
            self.spark_conf = self.spark_conf.set(k, v)
        
        # init spark context object
        self.spark_context = SparkContext(
            appName=self.app_name, 
            conf=self.spark_conf,
        )
        
        # init session
        self.session = SparkSession.builder.enableHiveSupport().getOrCreate()
        
        return self.session
    
    def kill(self, **kwargs):
        """
        function to kill spark session
        
        :return: None
        """
        self.session.stop()
        return None
    
    def write_parquet_part(
        self,
        sdf,
        parquet_dir,
        partition_info,
        n_partition=5,
        **kwargs
    ):
        """
        function to write a single partition
        as parquet file from spark dataframe
        
        :param sdf: spark dataframe spark session
        :param parquet_dir: str() of hdfs directory containing partitions
        :param partition_info: dict() of {partition_key : value} to write to,
            in correct order
        :param n_partition: int() of number of partitions within partition
        :return: None
        """
        write_mode = kwargs.get("mode", "overwrite")
        
        # apply partition information
        drop_part_cols = []
        parquet_path = str(parquet_dir)
        for _k, _v in partition_info.items():
            # construct directory path
            parquet_path = os.path.join(parquet_path, f"{_k}={_v}")
            # cache partition keys present in spark dataframe
            if _k in sdf.columns:
                drop_part_cols.append(_k)
                
        # drop partition keys present in spark dataframe
        sdf = sdf.drop(*drop_part_cols)
        
        # write
        self.write_parquet(
            sdf=sdf, 
            parquet_path=parquet_path,
            partition_cols=[],
            n_partition=n_partition,
            **kwargs
        )
        
        return None
    
    def write_parquet(
        self, 
        sdf, 
        parquet_path, 
        partition_cols=[], 
        n_partition=5, 
        **kwargs
    ):
        """
        function to write parquet file from spark dataframe
        
        :param sdf: spark dataframe in spark session
        :param parquet_path: str() of hdfs path
        :param partition_cols: list() of str() of column names
        :param n_partition: int() of number of partitions
        :return: None
        """
        write_mode = kwargs.get("mode", "overwrite")
        
        sdf.repartition(
            n_partition
        ).write.option(
            "header", "false"
        ).partitionBy(
            *partition_cols
        ).parquet(
            parquet_path, 
            mode=write_mode
        )
        
        return None
        
    def read_parquet(
        self, 
        parquet_path, 
        **kwargs):
        """
        function to read parquet to spark dataframe
        
        :param parquet_path: str() of hdfs path
        :param spark: pyspark.sql.session.SparkSession object
        :return: spark dataframe object
        """
        # spark session to use
        spark = kwargs.get("spark", self.session)
        # read
        sdf = spark.read.format(
            "parquet"
        ).option(
            "mergeSchema", "true"
        ).parquet(
            parquet_path
        )
        return sdf
    
    def drop_db_table(self, db, table, **kwargs):
        """
        function to drop table in database
        
        :param db: str() of database name
        :param table: str() of table name
        :param spark: pyspark.sql.session.SparkSession object
        :return: None
        """
        # spark session to use
        spark = kwargs.get("spark", self.session)
        # drop
        spark.sql(f"""
            DROP TABLE IF EXISTS {db}.{table}
        """)
        return None
    
    def write_db_table(
        self, 
        db, 
        table, 
        parquet_path,
        partition_cols=[],
        **kwargs,
    ):
        """
        function to write parquet table from hdfs to db
        
        :param db: str() of database name
        :param table: str() of table name
        :param parquet_path: str() of hdfs path
        :param partition_cols: list() of str() of column names
        :param spark: pyspark.sql.session.SparkSession object
        :return: None
        """
        # spark session to use
        spark = kwargs.get("spark", self.session)
        # read parquet from hdfs
        sdf = self.read_parquet(parquet_path, spark=spark)
        # write
        sdf.write.partitionBy(
            *partition_cols
        ).mode("overwrite").saveAsTable(
            f"{db}.{table}"
        )
        return None
    
    def repair_db_table(self, db, table, **kwargs):
        """
        function to repair db.table
        
        :param db: str() of database name
        :param table: str() of table name
        :param spark: pyspark.sql.session.SparkSession object
        :return: None
        """
        # spark session to use
        spark = kwargs.get("spark", self.session)
        # repair
        spark.sql(f"""
            MSCK REPAIR TABLE {db}.{table}
        """)
        return None
    
    def show_parts_db_table(self, db, table, **kwargs):
        """
        function to show partitions in db.table
        
        :param db: str() of database name
        :param table: str() of table name
        :param spark: pyspark.sql.session.SparkSession object
        :return: pandas dataframe
        """
        # spark session to use
        spark = kwargs.get("spark", self.session)
        return spark.sql(f"""
            SHOW PARTITIONS {db}.{table}
        """).toPandas()

    
def main():
    pass

    
if __name__ == "__main__":
    main()

