from pymongo import MongoClient
import pandas as pd

def extract_from_mongodb(uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]

    data = list(collection.find())
    for doc in data:
        doc.pop("_id", None)

    print(f" Extracted {len(data)} documents.")
    return pd.DataFrame(data)
