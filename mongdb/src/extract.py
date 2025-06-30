from pymongo import MongoClient
import pandas as pd
def read_data(db_name = 'Pipelines',collection_name = 'Project_Task'):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    data = list(collection.find({},{'_id':0}))
    return pd.DataFrame(data)

def normalize_technologies(df):
    return df.explode("technologies")