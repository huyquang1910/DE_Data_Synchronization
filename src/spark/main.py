from spark_write_data import SparkWriteDatabases
from config.database_config import get_database_config
from config.spark_config import SparkConnect
from pyspark.sql.types import *
from pyspark.sql.functions import col


def main():
    db_config = get_database_config()
    filejars = [
        "C:/Users/chimo/PycharmProjects/DE_ETL/lib/mysql-connector-j-9.3.0.jar"
    ]
    spark_conf = {
        "spark.jar.package": (
            "mysql:mysql-connector-java:9.3.0"
        )
    }

    spark_write_database = SparkConnect(
        app_name= 'de',
        master_url='local[*]',
        executor_memory= '1g',
        executor_cores= 1,
        drive_memory= '1g',
        num_executors= 1,
        jars= filejars,
        spark_conf= spark_conf,
        log_level= 'INFO'
    ).spark

    schema = StructType([
        StructField('actor', StructType([
            StructField('id', LongType(), False),
            StructField('login', StringType(), False),
            StructField('gravatar_id', StringType(), False),
            StructField('url', StringType(), False),
            StructField('avatar_url', StringType(), False),
        ]), True),
        StructField('repo', StructType([
            StructField('id', LongType(), False),
            StructField('name', StringType(), True),
            StructField('url', StringType(), True),
        ]), True)
    ])
    df = spark_write_database.read.schema(schema).json("C:/Users/chimo/PycharmProjects/DE_ETL/data/2015-03-01-17.json")
    df_write_table_Users = df.select(
        col("actor.id"). alias("user_id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
    )
    df_write_table_Repositories = df.select(
        col("repo.id").alias("repo_id"),
        col("repo.name").alias("name"),
        col("repo.url").alias("url"),
    )
    df.show(truncate = False)

if __name__== "__main__":
    main()