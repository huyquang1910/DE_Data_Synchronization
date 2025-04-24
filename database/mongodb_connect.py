from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from config.database_config import get_database_config
from database.schema_manager import create_mongodb_schema, validate_mongodb_schema
from bson.int64 import Int64


#step1: get mongodb config
#step2: def connect
#step3: def disconnect
#step4: def reconnect
#setep5: def exit

class MongoDBConnect:
    #step1
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name =  db_name
        self.client = None
        self.db = None
    def connect(self):
        try:
            self.client = MongoClient(self.mongo_uri)
            self.client.server_info() #test connect
            self.db =  self.client[self.db_name]
            # print(self.client.server_info())
            print(f"------Connected to MongoDB : {self.db_name}-----------")
            return self.db
        except ConnectionFailure as e:
            raise Exception(f"------Failed to connected MongoDB: {e}") from e
    def close(self):
        if self.client:
            self.client.close()
        print("-------------Mongo connection closed---------")
    def __enter__(self):
        self.connect()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
