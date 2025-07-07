import configparser
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
 
def load_mysql(data1,data2):
   
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
   
         
         data1.to_sql(name='sorted_orders',con=engine,if_exists='replace',index=False)
         data2.to_sql(name='sorted_orders',con=engine,if_exists='replace',index=False)
