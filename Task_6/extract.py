import configparser
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
def extract_mss():

        config = configparser.ConfigParser()
        config.read(r'C:\Users\Rahul\Documents\Visual Studio 2017\python\config.config')
        
        username = config['SqlDB']['username']
        password = config['SqlDB']['password']
        server     = config['SqlDB']['server']
        database = config['SqlDB']['database']
        driver   = config['SqlDB']['driver']
        
        encoded_password = urllib.parse.quote_plus(password)
        
       # Encode parameters for SQLAlchemy
        params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={encoded_password}"
             )
 
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        
        
        #fetch orders table from MYSQLDB
        query = "SELECT * FROM customers_scd" 
        df = pd.read_sql(query, con=engine)
        print(df.head())

        return df
if __name__ == "__main__":
    extract_mss() 

        