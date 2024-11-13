import json
import requests
from requests.exceptions import HTTPError, Timeout, RequestException
import json
import boto3
from datetime import datetime
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
    
    try:
        response = requests.get(url)
        
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        
        data = response.json()
        
        #dumping data to s3
        s3_client = boto3.client('s3')
        file_timestamp = datetime.now()
        bucket_name = 'cryptoinsight-etl-project-bucket'
        folder = 'raw_data/'
        filename = f"cryptoinsight_raw_{file_timestamp}"
        
        s3_client.put_object(
            Body = json.dumps(data),
            Bucket = bucket_name,
            Key=f"{folder}{filename}"
        )
        
    except ClientError as e:
        print(f"ClientError: Failed to upload data to S3: {e}")
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Timeout as timeout_err:
        print(f"Request timed out: {timeout_err}")
    except RequestException as req_err:
        print(f"Error occurred during the request: {req_err}")
    except Exception as err:
        print(f"An unexpected error occurred: {err}")
    
    return None
    