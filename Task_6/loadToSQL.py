import configparser
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
 
def load_to_ssm(data1,data2,data3,data4,data5):
   
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
   
    
    data1.to_sql(name='scd1',con=engine,if_exists='replace',index=False)
    data2.to_sql(name='scd2',con=engine,if_exists='replace',index=False)
    data3.to_sql(name='scd3',con=engine,if_exists='replace',index=False)
    data4.to_sql(name='scd4',con=engine,if_exists='replace',index=False)
    data5.to_sql(name='scd5',con=engine,if_exists='replace',index=False)
