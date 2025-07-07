import json
from pymongo import MongoClient

def insert_txt_to_mongodb(txt_file_path, mongo_uri, db_name, collection_name):
    with open(txt_file_path, "r") as file:
        data = json.load(file)  # Assumes the file contains a JSON array

    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    collection.insert_many(data)
    print(f"Inserted {len(data)} records into MongoDB collection: {collection_name}")
