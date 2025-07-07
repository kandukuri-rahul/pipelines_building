import pandas as pd
def transfrom(df):

    #sorting 
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')  # Adjust column name if different
    organized_df = df.sort_values(by='order_date')
    #aggregation
    sales_df = df.groupby('customer_id').agg({
        'order_id': 'count',
        'order_amount': 'sum'
    }).reset_index()
    sales_df.rename(columns={
        'order_id': 'total_orders',
        'order_amount': 'total_sales'
    }, inplace=True)
    
    
   
    return organized_df,sales_df
    
 