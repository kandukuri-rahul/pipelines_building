
import pandas as pd
from datetime import datetime

#  SCD Type 1: Overwrite current values
def scd_type_1(current_df: pd.DataFrame, incoming_df: pd.DataFrame) -> pd.DataFrame:
    df = current_df.copy()
    for _, row in incoming_df.iterrows():
        df.loc[df['CustomerID'] == row.CustomerID,
               ['Email', 'Phone', 'Address', 'City']] = row[['Email', 'Phone', 'Address', 'City']].values
    return df


#  SCD Type 2: End current record, insert new with Effective/End dates
def scd_type_2(current_df: pd.DataFrame, incoming_df: pd.DataFrame) -> pd.DataFrame:
    updated_df = current_df.copy()
    now = datetime.now().strftime('%Y-%m-%d') 

    for _, inc_row in incoming_df.iterrows():
        match = updated_df[(updated_df['CustomerID'] == inc_row.CustomerID) & (updated_df['IsCurrent'] == 1)]
        if not match.empty:
            for col in ['Email', 'Phone', 'Address', 'City']:
                if inc_row[col] != match.iloc[0][col]:
                    updated_df.loc[match.index, 'EndDate'] = now
                    updated_df.loc[match.index, 'IsCurrent'] = 0
                    new_row = inc_row.to_dict()
                    new_row.update({'EffectiveDate': now, 'EndDate': None, 'IsCurrent': 1})
                    updated_df = pd.concat([updated_df, pd.DataFrame([new_row])], ignore_index=True)
                    break
        else:
            new_row = inc_row.to_dict()
            new_row.update({'EffectiveDate': now, 'EndDate': None, 'IsCurrent': 1})
            updated_df = pd.concat([updated_df, pd.DataFrame([new_row])], ignore_index=True)
    return updated_df


#  SCD Type 3: Add a column to store previous value (limited history)
def scd_type_3(current_df: pd.DataFrame, incoming_df: pd.DataFrame) -> pd.DataFrame:
    df = current_df.copy()
    if 'PreviousCity' not in df.columns:
        df['PreviousCity'] = None

    for _, inc_row in incoming_df.iterrows():
        idx = df[df['CustomerID'] == inc_row.CustomerID].index
        if not idx.empty:
            if df.loc[idx[0], 'City'] != inc_row.City:
                df.loc[idx[0], 'PreviousCity'] = df.loc[idx[0], 'City']
                df.loc[idx[0], 'City'] = inc_row.City
    return df


# SCD Type 4: Maintain full history in a separate table
def scd_type_4(current_df: pd.DataFrame, incoming_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    history = []
    df = current_df.copy()
    for _, inc_row in incoming_df.iterrows():
        idx = df[(df['CustomerID'] == inc_row.CustomerID)].index
        if not idx.empty:
            for col in ['Email', 'Phone', 'Address', 'City']:
                if df.loc[idx[0], col] != inc_row[col]:
                    history.append(df.loc[idx[0]].to_dict())
                    df.loc[idx[0], col] = inc_row[col]
    history_df = pd.DataFrame(history)
    return df, history_df


#  SCD Type 5: Hybrid of Type 1 and Type 4
def scd_type_5(current_df: pd.DataFrame, incoming_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    updated_df, history_df = scd_type_4(current_df, incoming_df)
    return updated_df, history_df
