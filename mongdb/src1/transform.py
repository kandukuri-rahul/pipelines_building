import pandas as pd

def transform_projects(df):
    df_flat = pd.json_normalize(df.to_dict(orient='records'), sep='_')

    # Print available columns for debugging
    print("Available columns:", df_flat.columns.tolist())

    # Map available columns
    expected_columns = {
        'project_id': 'project_id',
        'project_name': 'project_name',
        'technologies': 'technologies',
        'status': 'status',
        'client_name': 'client.name',
        'client_industry': 'client.industry',
        'client_city': 'client.location.city',
        'client_country': 'client.location.country',
        'project_manager': 'team.project_manager'
    }

    actual_columns = {}
    for new_name, raw_key in expected_columns.items():
        if raw_key in df_flat.columns:
            actual_columns[raw_key] = new_name

    # Rename and select available columns
    project_df = df_flat[list(actual_columns.keys())].rename(columns=actual_columns)

    return project_df

def transform_team_members(df):
    df_team = pd.json_normalize(
        df.to_dict(orient='records'),
        record_path=['team', 'members'],
        meta=['project_id'],
        sep='_',
        errors='ignore'
    )

    df_team.rename(columns={
        'name': 'member_name',
        'role': 'member_role'
    }, inplace=True)

    return df_team

def transform_milestones(df):
    df_milestones = pd.json_normalize(
        df.to_dict(orient='records'),
        record_path='milestones',
        meta=['project_id'],
        sep='_',
        errors='ignore'
    )

    df_milestones.rename(columns={
        'name': 'milestone_name',
        'due_date': 'milestone_due_date'
    }, inplace=True)

    return df_milestones
