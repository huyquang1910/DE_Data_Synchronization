from wsgiref.validate import validator
from pathlib import Path
from mysql.connector import Error

def create_mongodb_schema(db):
    db.drop_collection("Users")
    db.create_collection("Users", validator={
        "$jsonSchema":{
            "bsonType": "object",
            "required": ["user_id", "login"],
            "properties":{
                "user_id":{
                    "bsonType": "long"
                },
                "login":{
                    "bsonType":"string"
                },
                "gravatar_id":{
                    "bsonType": ["string","null"]
                },
                "avatar_url":{
                    "bsonType":["string","null"]
                },
                "url":{
                    "bsonType": ["string","null"]
                }
            }
    }
    })
    db.Users.create_index("user_id", unique = True)
def validate_mongodb_schema(db):
    collections= db.list_collection_names()
    # print(collections)
    if "Users" not in collections:
        raise ValueError(f"-----------Missing collection in Mongo db------------")
    user= db.Users.find_one({"user_id": 1})
    if not user:
        raise ValueError(f"----------user_id not found in MongoDB")
    print(f"--------Validate schema in MongoDB---------")

SQL_FILE_PATH= Path("../sql/schema.sql")
def create_mysql_schema(connection, cursor):
    database= "github_data"
    cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    print(f"----Created database {database} in MySQL----")
    connection.database= database
    try:
        with open(SQL_FILE_PATH, "r") as file:
            sql_script = file.read()
            sql_commands= [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            for cmd in sql_commands:
                cursor.execute(cmd)
                print(f"-----Executed MySQL commands--------")
            connection.commit()
    except Error as e :
        connection.rollback()
        raise Exception(f"-----Failed to created MySQL schema: {e}") from e

def validate_mysql_schema(cursor):
    cursor.execute("SHOW TABLES")
    tables= [row[0] for row in cursor.fetchall()]
    if "Users" not in tables or "Repositories" not in tables:
        raise ValueError(f"-------Table doesn't not exist------")
    cursor.execute("SELECT * FROM Users where user_id = 1")
    # print(cursor.fetchone())
    user= cursor.fetchone()
    if not user:
        raise ValueError("------user not found------")
    print("-----validate schema in MySQL")

def create_redis_schema(client):
    try:
        client.flushdb() # drop db
        #         "user_id":Int64(1),
        #         "login":"ostcar",
        #         "gravatar_id":"",
        #         "avatar_url": "https://avatars.githubusercontent.com/u/977937?",
        #         "url":"https://api.github.com/users/ostcar",
        client.set("user:1:login","ostcar")
        client.set("user:1:gravatar_id", "")
        client.set("user:1:avatar_url","https://avatars.githubusercontent.com/u/977937?")
        client.set("user:1:url","https://api.github.com/users/ostcar" )
        client.sadd("user_id", "user:1")
        print("------Add data to Redis successfully-----")
    except Exception as e:
        raise Exception(f"--------Failed to add data to Redis:{e} -----") from e

def validate_redis_schema(client):
    if not client.get("user:1:login") == "ostcar":
        raise ValueError("-----Value login not found in Redis----")
    if not client.sismember("user_id", "user:1"):
        raise ValueError("-----Value user_id not set in Redis -----")
    print("-----validated schema in Redis-----")

