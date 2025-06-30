import urllib
from sqlalchemy import create_engine

def load_to_sqlserver(df, server, database, username, password, table):
    connection_string = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")
    df.to_sql(table, con=engine, if_exists="replace", index=False)
    print(f"Loaded data into SQL Server table: {table}")


import configparser
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd

def load_to_ssms(data):
    config = configparser.ConfigParser()
    config.read(r'C:\Users\Rahul\Documents\Visual Studio 2017\python\config.config')

    username = config['SqlDB']['username']
    password = config['SqlDB']['password']
    server   = config['SqlDB']['server']
    database = config['SqlDB']['database']
    driver   = config['SqlDB']['driver']

    params = urllib.parse.quote_plus(
        f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    )

    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

    data.to_sql('data_from_MongoDB',con=engine,if_exists = 'replace',index = False)

    