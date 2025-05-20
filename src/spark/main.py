from spark_write_data import SparkWriteDatabases
from config.database_config import get_database_config
from config.spark_config import SparkConnect
from pyspark.sql.types import *
from pyspark.sql.functions import col
from config.spark_config import get_spark_config


def main():
    db_configs = get_database_config()

    jar = [
        db_configs["mysql"].jar_path
    ]



    # filejars = [
    #     "C:/Users/chimo/PycharmProjects/DE_ETL/lib/mysql-connector-j-9.3.0.jar"
    # ]
    # spark_conf = {
    #     "spark.jar.package": (
    #         "mysql:mysql-connector-java:9.3.0"
    #     )
    # }

    spark_connect = SparkConnect(
        app_name= 'de',
        master_url='local[*]',
        executor_memory= '1g',
        executor_cores= 1,
        drive_memory= '1g',
        num_executors= 1,
        jars= jar,
        # spark_conf= spark_conf,
        log_level= 'INFO'
    )

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
    df = spark_connect.spark.read.schema(schema).json("C:/Users/chimo/PycharmProjects/DE_ETL/data/2015-03-01-17.json")
    df_write_table_Users = df.select(
        col("actor.id"). alias("user_id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.avatar_url").alias("avatar_url"),
        col("actor.url").alias("url"),
    )
    df_write_table_Repositories = df.select(
        col("repo.id").alias("repo_id"),
        col("repo.name").alias("name"),
        col("repo.url").alias("url"),
    )
    # df.show(truncate = False)

    spark_configs = get_spark_config()
    df_write = SparkWriteDatabases(spark_connect.spark, spark_configs)
    df_write.write_all_databases(df_write_table_Users, mode = "append")

    spark_connect.spark.stop()
if __name__== "__main__":
    main()