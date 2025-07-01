import configparser
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
def extract():

        config = configparser.ConfigParser()
        config.read(r'C:\Users\Rahul\Documents\Visual Studio 2017\python\config.config')
        
        username = config['MySqlDB']['username'] 
        password = config['MySqlDB']['password']
        host     = config['MySqlDB']['host']
        database = config['MySqlDB']['database']
        driver   = config['MySqlDB']['driver']
        
        encoded_password = urllib.parse.quote_plus(password)
        
        connection_string = f"mysql+{driver}://{username}:{encoded_password}@{host}/{database}"
        engine = create_engine(connection_string)
        
        
        #fetch orders table from MYSQLDB
        fetched_order_table = pd.read_sql_table('orders',con=engine)
        
        return fetched_order_table
        
        