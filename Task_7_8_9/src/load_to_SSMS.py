import configparser
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
 
def load_to_ssm(organized_df,sales_df):
   
    #read config
    config = configparser.ConfigParser()
    config.read(r'C:\Users\Rahul\Documents\Visual Studio 2017\python\config.config')
 
    #load credentials
    username = config['SqlDB']['username']
    password = config['SqlDB']['password']
    server = config['SqlDB']['server']
    database = config['SqlDB']['database']
    driver = config['SqlDB']['driver']
 
    params = urllib.parse.quote_plus(
        f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    )
 
 
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
   
    
    organized_df.to_sql(name='sorted_data',con=engine,if_exists='replace',index=False)
    sales_df.to_sql(name='aggregated_data',con=engine,if_exists='replace',index=False)
