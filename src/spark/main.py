from spark_write_data import SparkWriteDatabases
from config.database_config import get_database_config
from config.spark_config import SparkConnect

def main():
    db_config = get_database_config()
    filejars = [
        "../lib/mysql-connector-j-9.3.0.jar"
    ]
    spark_conf ={
        "spark.jar.pak"
    }

    spark_write_database = SparkConnect(
        app_name= 'de',
        master_url='local[*]',
        executor_memory= '4g',
        executor_cores= 2,
        drive_memory= '2g',
        num_executors= 3,
        jars= filejars,
        spark_conf= None,
        log_level= 'INFO'
    )