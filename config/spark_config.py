from pyspark.sql import SparkSession
from typing import Optional, List, Dict
import os

class SparkConnect:
    def __init__(self,
                 app_name: str,
                 master_url: str = "local[*]",
                 executor_memory: Optional[str] = "4g",
                 executor_cores: Optional[int] = 2,
                 drive_memory: Optional[str] = "2g",
                 num_executors: Optional[int] = 3,
                 jars: Optional[List[str]] = None,
                 spark_conf: Optional[Dict[str, str]] = None,
                 log_level: str = "WARN"
                 ):
        self.app_name = app_name
        self.spark = self.create_spark_session(master_url, executor_memory, executor_cores, drive_memory, num_executors, jars, spark_conf, log_level)


    def create_spark_session(
            self,
            #app_name: str,
            master_url: str = "local[*]",
            executor_memory: Optional[str]="4g",
            executor_cores: Optional[int]=2,
            drive_memory: Optional[str]= "2g",
            num_executors: Optional[int]= 3,
            jars: Optional[List[str]]=None,
            spark_conf: Optional[Dict[str, str]]= None,
            log_level: str= "WARN"
    ) ->SparkSession:
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(master_url)

        if executor_memory:
            builder.config("spark.executor.memory", executor_memory)
        if executor_cores:
            builder.config("spark.executor.cores", executor_cores)
        if drive_memory:
            builder.config("spark.driver.memory", drive_memory)
        if num_executors:
            builder.config("spark.executor.instances", num_executors)
        if jars:
            jars_path= ",".join([os.path.abspath(jar) for jar in jars])
            builder.config("spark.jars", jars_path)
        if spark_conf:
            for key, value in spark_conf.items():
                builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)
        return spark

    def stop(self):
        if self.spark:
            self.spark.stop()
            print("-------Stop spark session------")

    # spark =  create_spark_session(
    #     app_name = "DE_ETL",
    #     master_url = "local[*]",
    #     executor_memory = "4g",
    #     executor_cores = 2,
    #     drive_memory = "2g",
    #     num_executors = 4,
    #     jars = None,
    #     spark_conf = {"spark.sql.shuffle.partitions": "10"},
    #     log_level = "DEBUG"
    # )
    # data= [["dat",18],["sonbui",20],["toan",25]]
    # df=spark.createDataFrame(data,["name","age"])
    # df.show()

