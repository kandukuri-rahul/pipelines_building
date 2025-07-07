from insert_to_mongodb import insert_txt_to_mongodb
from extract import extract_from_mongodb
from transform import transform_projects, transform_team_members, transform_milestones
from load import load_to_sqlserver
import configparser

def main():
    # ------------------- SQL Server Config -------------------
    config = configparser.ConfigParser()
    config.read("config.config")

    sql_server = config["SqlDB"]["server"]
    sql_db = config["SqlDB"]["database"]
    sql_user = config["SqlDB"]["username"]
    sql_pass = config["SqlDB"]["password"]

    # ------------------- MongoDB Hardcoded Config -------------------
    mongo_uri = "mongodb://localhost:27017"
    mongo_db = "project_db"
    mongo_collection = "nested_projects"
    txt_file_path = r"C:\Users\Rahul\Documents\Visual Studio 2017\python\mongdb\src1\Doc_unstructured_1.txt"

    # ------------------- ETL Flow -------------------
    insert_txt_to_mongodb(txt_file_path, mongo_uri, mongo_db, mongo_collection)
    df_raw = extract_from_mongodb(mongo_uri, mongo_db, mongo_collection)

    df_projects = transform_projects(df_raw)
    df_team = transform_team_members(df_raw)
    df_milestones = transform_milestones(df_raw)

    # Explode technologies into separate rows
    if 'technologies' in df_projects.columns:
        df_projects = df_projects.explode('technologies').reset_index(drop=True)

    load_to_sqlserver(df_projects, sql_server, sql_db, sql_user, sql_pass, "projects")
    load_to_sqlserver(df_team, sql_server, sql_db, sql_user, sql_pass, "project_team")
    load_to_sqlserver(df_milestones, sql_server, sql_db, sql_user, sql_pass, "project_milestones")

if __name__ == "__main__":
    main()
