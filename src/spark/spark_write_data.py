from pyspark.sql import DataFrame, SparkSession
from typing import Dict
from database.mysql_connect import MySQLConnect

class SparkWriteDatabases:
    def __init__(self, spark: SparkSession, db_config: Dict):
        self.spark =  spark
        self.db_config = db_config

    def spark_write_mysql(self, df: DataFrame, table_name: str, jdbc_url: str, config: Dict, mode : str = "append"):

        try:
            mysql_client = MySQLConnect(config)
            mysql_client.connect()
            mysql_client.close()
        except Exception as e:
            raise Exception(f"----Failed to connect MySQL: {e}------") from e

        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .mode(mode) \
            .save()
        print(f"-----Spark write data to mysql in table :{table_name}------")