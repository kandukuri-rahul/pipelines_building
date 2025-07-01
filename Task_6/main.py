from extract import extract_mss
from scdTypes import scd_type_1, scd_type_2, scd_type_3, scd_type_4, scd_type_5
import pandas as pd

def main():
    # 🔹 1. Extract current data from SQL Server
    current_df = extract_mss()

    # 🔹 2. Read new data from CSV
    incoming_df = pd.read_csv(r'c:\Users\Rahul\Downloads\incoming_customers.csv')  # or your CSV path

    # 🔹 3. Apply any SCD type
    df_scd1 = scd_type_1(current_df, incoming_df)
    df_scd2 = scd_type_2(current_df, incoming_df)
    df_scd3 = scd_type_3(current_df, incoming_df)
    df_scd4, history4 = scd_type_4(current_df, incoming_df)
    df_scd5, history5 = scd_type_5(current_df, incoming_df)

    # 🔹 4. Print or save results
    print("SCD Type 2 Result:")
    print(df_scd2)
    print("SCD Type 1 Result:")
    print(df_scd1)
    print("SCD Type 3 Result:")
    print(df_scd3)
    print("SCD Type 4 Result:")
    print(df_scd4) 
    print("SCD Type 5 Result:")
    print(df_scd5)

if __name__ == "__main__":
    main()
