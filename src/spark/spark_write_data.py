from pyspark.sql import DataFrame, SparkSession
from typing import Dict
from database.mysql_connect import MySQLConnect
from config.spark_config import get_spark_config


class SparkWriteDatabases:
    def __init__(self, spark: SparkSession, db_config: Dict):
        self.spark =  spark
        self.db_config = db_config

    def spark_write_mysql(self, df_write: DataFrame, table_name: str, jdbc_url: str, config: Dict, mode : str = "append"):
        # try:
        #     mysql_client = MySQLConnect(config)
        #     mysql_client.connect()
        #     mysql_client.close()
        # except Exception as e:
        #     raise Exception(f"----Failed to connect MySQL: {e}------") from e

        try:
            with MySQLConnect(config["host"], config["port"], config["user"], config["password"]) as mysql_client:
                connection, cursor = mysql_client.connection, mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN spark_temp VARCHAR(255)")
                connection.commit()
                print("-----Add column spark_temp to MySQL------")
                mysql_client.close()
        except Exception as e:
            raise Exception(f"--------Fail to connect Mysql: {e}-----------")

        df_write.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .mode(mode) \
            .save()
        print(f"-----Spark write data to mysql in table :{table_name}------")

    def validate_spark_mysql(self,df_write: DataFrame,table_name: str, jdbc_url: str, config: Dict, mode: str = "append"):

        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE spark_temp = 'spark_write') as temp" ) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .load()
        print(df_read.count())

        def subtract_dataframe(df_write: DataFrame, df_read: DataFrame):
            result = df_write.exceptAll(df_read)
            print(f"---result records:{result.count()} --- ")
            if not result.isEmpty():
                result.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .option("dbtable", table_name) \
                    .option("user", config["user"]) \
                    .option("password", config["password"]) \
                    .mode(mode) \
                    .save()

        if (df_write.count()) == (df_read.count()):
            print(f"----validate {df_write.count()} records success------")
            subtract_dataframe(df_write, df_read)
            print(f"-------validate data of records success----")
        else:
            subtract_dataframe(df_write, df_read)
            print("----Insert missing records success by using Spark-------")

        try:
            with MySQLConnect(config["host"], config["port"], config["user"], config["password"]) as mysql_client:
                connection, cursor = mysql_client.connection, mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN spark_temp")
                connection.commit()
                print("-----Drop column spark_temp in MySQL------")
                mysql_client.close()
        except Exception as e:
            raise Exception(f"--------Fail to connect Mysql: {e}-----------")

    def spark_write_mongodb(self,df: DataFrame, database:str, collection:str, uri: str, mode: str = "append"):
        df.write \
            .format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .mode(mode) \
            .save()
        print(f"-----Spark write data to MongoDB in collection :{database}.{collection}------")

    def write_all_databases(self, df: DataFrame, mode: str = "append"):
        self.spark_write_mysql(
            df,
            self.db_config["mysql"]["table"],
            self.db_config["mysql"]["jdbc_url"],
            self.db_config["mysql"]["config"],
            mode
        )
        self.spark_write_mongodb(
            df,
            self.db_config["mongodb"]["database"],
            self.db_config["mongodb"]["collection"],
            self.db_config["mongodb"]["uri"],
            mode

        )
        print("-------Write success to all databases-------")