from config.database_config import get_database_config
from database.mysql_connect import MySQLConnect
from database.schema_manager import create_mongodb_schema, validate_mongodb_schema, validate_mysql_schema
from bson.int64 import Int64
from database.mongodb_connect import MongoDBConnect
from database.redis_connect import RedisConnect
from  database.schema_manager import create_mysql_schema
from database.schema_manager import create_redis_schema, validate_redis_schema

def main(config):
    # MongoDB
    with MongoDBConnect(config["mongodb"].uri,config["mongodb"].db_name) as mongo_client:
        create_mongodb_schema(mongo_client.connect())
        print("-----Connected to MongoDB----")
        # mongo_client.db.Users.insert_one({
        #     "user_id":Int64(1),
        #     "login":"ostcar",
        #     "gravatar_id":"",
        #     "avatar_url": "https://avatars.githubusercontent.com/u/977937?",
        #     "url":"https://api.github.com/users/ostcar",
        #
        # })
        # print("---------Inserted to MongoDB------")
        # validate_mongodb_schema(mongo_client.connect())

    #MySQL
    with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user, config["mysql"].password) as mysql_client:
        connection = mysql_client.connection
        cursor = mysql_client.cursor
        create_mysql_schema(connection, cursor)
        cursor.execute(f"INSERT INTO Users(user_id, login, gravatar_id,avatar_url, url ) VALUES (%s, %s, %s, %s, %s)", (1,"huyquang1910","https://avatars.githubusercontent.com/u/977937?", "https://api.github.com/users/ostcar",""))
        connection.commit()
        print("------Inserted data to MySQL-------")
        validate_mysql_schema(cursor)

    #
    #Redis
    # with RedisConnect(config["redis"].host,config["redis"].port,config["redis"].user,config["redis"].password,config["redis"].database ) as redis_client:
    #     create_redis_schema(redis_client.connect())
    #     validate_redis_schema(redis_client.connect())
if __name__== "__main__":
    config = get_database_config()
    main(config)