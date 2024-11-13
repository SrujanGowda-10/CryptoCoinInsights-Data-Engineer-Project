import json
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

# Convert into csv and load it into bucket
def convert_and_upload_csv_to_bucket(df, Key):
    Bucket='cryptoinsight-etl-project-bucket'
    s3_client = boto3.client('s3')
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    data = buffer.getvalue()
    s3_client.put_object(
        Bucket=Bucket,
        Key=Key,
        Body=data
        )

def lambda_handler(event, context):
    
    s3_client = boto3.client('s3')
    bucket_name = 'cryptoinsight-etl-project-bucket'
    src_file_path = 'raw_data/'
    
    #all objects in a bucket
    objects = s3_client.list_objects(Bucket=bucket_name).get('Contents')
    
    src_keys = []
    json_data = []
    
    for obj in objects:
        if obj['Key'].startswith('raw_data/cryptoinsight_raw_'):
            
            #get object/file
            response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
            str_data = response['Body'].read()
            
            #converting string json into python dict object
            data = json.loads(str_data)
            
            df = pd.json_normalize(data)
            
            # coin info
            coin_df = df[['symbol', 'name', 'image']]
            coin_df['id'] = coin_df['symbol']
            coin_df = coin_df.rename(columns = {'image':'image_url'})
            
            coin_df_col_order = ['id','name', 'symbol', 'image_url']
            coin_df = coin_df[coin_df_col_order]
            
            #coin metrics info
            coin_metrics_df = df[['symbol', 'current_price', 'market_cap', 'market_cap_rank', 'total_volume','price_change_percentage_24h', 'market_cap_change_percentage_24h','high_24h', 'low_24h', 'price_change_24h', 'circulating_supply','total_supply', 'max_supply', 'last_updated']]
            # coin_metrics_df['id'] = coin_metrics_df['symbol']
            coin_metrics_df = coin_metrics_df.rename(columns={'current_price':'current_price_usd','symbol':'id'})
            coin_metrics_df['last_updated'] = pd.to_datetime(coin_metrics_df['last_updated'])
            
            coin_metrics_df_col_order = ['id', 'current_price_usd', 'market_cap', 'market_cap_rank', 'total_volume','price_change_percentage_24h', 'market_cap_change_percentage_24h','high_24h', 'low_24h', 'price_change_24h', 'circulating_supply','total_supply', 'max_supply', 'last_updated']
            coin_metrics_df = coin_metrics_df[coin_metrics_df_col_order]
            
            #loading the csv files into transformed bucket
            file_timestamp = datetime.now()
            coin_key_path = f"transformed_data/coin_info/coin_info_{file_timestamp}.csv"
            convert_and_upload_csv_to_bucket(df=coin_df, Key=coin_key_path)
            
            coin_metrics_key_path = f"transformed_data/coin_metrics/coin_metrics_{file_timestamp}.csv"
            convert_and_upload_csv_to_bucket(df=coin_metrics_df, Key=coin_metrics_key_path)
            

