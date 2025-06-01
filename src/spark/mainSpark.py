from spark_write_data import SparkWriteDatabases
from config.database_config import get_database_config
from config.spark_config import SparkConnect
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit
from config.spark_config import get_spark_config


def main():
    db_configs = get_database_config()

    # jar = [
    #     db_configs["mysql"].jar_path,
    #     db_configs["mongodb"].jar_path,
    # ]


    jars = [
        "mysql:mysql-connector-java:8.0.33",
        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
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
        jar_packages= jars,
        # spark_conf= spark_conf,
        log_level= 'INFO'
    )

    schema = StructType([
        StructField('actor', StructType([
            StructField('id', IntegerType(), False),
            StructField('login', StringType(), True),
            StructField('gravatar_id', StringType(), True),
            StructField('url', StringType(), True),
            StructField('avatar_url', StringType(), True),
        ]), True),
        StructField('repo', StructType([
            StructField('id', LongType(), False),
            StructField('name', StringType(), True),
            StructField('url', StringType(), True),
        ]), True)
    ])
    df = spark_connect.spark.read.schema(schema).json("C:/Users/chimo/PycharmProjects/DE_ETL/data/2015-03-01-17.json")
    # df_write_table_Users = df.select(
    #     col("actor.id"). alias("user_id"),
    #     col("actor.login").alias("login"),
    #     col("actor.gravatar_id").alias("gravatar_id"),
    #     col("actor.avatar_url").alias("avatar_url"),
    #     col("actor.url").alias("url"),
    # )
    # df_write_table_Users.cache()
    df_write_table_Repositories = df.select(
        col("repo.id").alias("repo_id"),
        col("repo.name").alias("name"),
        col("repo.url").alias("url"),
    )
    # df.show(truncate = False)
    df_write_table_Users = df.withColumn("spark_temp", lit("spark_write")).select(
        col("actor.id"). alias("user_id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.avatar_url").alias("avatar_url"),
        col("actor.url").alias("url"),
        col("spark_temp").alias("spark_temp")
    )
    spark_configs = get_spark_config()
    df_write = SparkWriteDatabases(spark_connect.spark, spark_configs)
    df_write.write_all_databases(df_write_table_Users, mode = "append")

    df_validate = SparkWriteDatabases(spark_connect.spark, spark_configs)
    df_validate.validate_spark_mysql(df_write_table_Users,spark_configs["mysql"]["table"],spark_configs["mysql"]["jdbc_url"],spark_configs["mysql"]["config"])
    spark_connect.stop()
if __name__== "__main__":
    main()