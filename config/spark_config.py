from pyspark.sql import SparkSession
from typing import Optional, List, Dict
import os
from config.database_config import get_database_config

class SparkConnect:
    def __init__(self,
                 app_name: str,
                 master_url: str = "local[*]",
                 executor_memory: Optional[str] = "4g",
                 executor_cores: Optional[int] = 2,
                 drive_memory: Optional[str] = "2g",
                 num_executors: Optional[int] = 3,
                 jar_packages: Optional[List[str]] = None,
                 spark_conf: Optional[Dict[str, str]] = None,
                 log_level: str = "WARN"
                 ):
        self.app_name = app_name
        self.spark = self.create_spark_session(master_url, executor_memory, executor_cores, drive_memory, num_executors,  jar_packages, spark_conf, log_level)


    def create_spark_session(
            self,
            #app_name: str,
            master_url: str = "local[*]",
            executor_memory: Optional[str]="4g",
            executor_cores: Optional[int]=2,
            drive_memory: Optional[str]= "2g",
            num_executors: Optional[int]= 3,
            jar_packages: Optional[List[str]]=None,
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
        # if jars:
        #     jars_path= ",".join([os.path.abspath(jar) for jar in jars])
        #     builder.config("spark.jars", jars_path)
        if jar_packages:
            jar_packages_url = ",".join([jar_package for jar_package in jar_packages])
            builder.config("spark.jars.packages", jar_packages_url)

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
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
def get_spark_config() -> Dict:
    db_configs = get_database_config()
    return {
        "mysql":{
            "table": db_configs["mysql"].table,
            "jdbc_url": "jdbc:mysql://{}:{}/{}".format(db_configs["mysql"].host,db_configs["mysql"].port, db_configs["mysql"].database),
            "config": {
                "host": db_configs["mysql"].host,
                "port": db_configs["mysql"].port,
                "user": db_configs["mysql"].user,
                "password": db_configs["mysql"].password,
                "database": db_configs["mysql"].database,
            }
        },
        "mongodb":{
            "database": db_configs["mongodb"].db_name,
            "collection": db_configs["mongodb"].collection,
            "uri": db_configs["mongodb"].uri
        },
        "redis":{


        }
    }
#
# if __name__== "__main__":
#     print(get_spark_config())
