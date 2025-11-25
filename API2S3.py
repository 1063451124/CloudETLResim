import json
import requests
import os
import boto3
from datetime import datetime

# 这个Lambda不再需要mysql.connector库，因此移除相关import 
# Source 到 ODS

def lambda_handler(event, context):
    print(f"=== Lambda execution started (RequestId: {context.aws_request_id}) ===")
    
    # 1. Get data from Alpha Vantage
    api_key = os.getenv("ALPHAVANTAGE_API_KEY", "demo")
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": "MSFT",
        "apikey": api_key
    }

    try:
        print(f"[1] Starting API request: {url}")
        response = requests.get(url, params=params, timeout=10)
        print(f"[1] API response status code: {response.status_code}")
        
        data = response.json()
        time_series = data.get("Time Series (Daily)", {})
        print(f"[1] Extracted {len(time_series)} stock data points")
        
        first_two = dict(list(time_series.items())[:2])
        
        s3_key = "" # Initialize s3_key
        if first_two:
            print(f"[2] Calling save_to_s3 to store data")
            s3_key = save_to_s3(first_two, "MSFT")
        else:
            print("No data to store")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data successfully stored to S3",
                "data_count": len(first_two),
                "s3_key": s3_key # Return the key for Lambda2 to use
            })
        }

    except Exception as e:
        print(f"[ERROR] Lambda execution failed: {str(e)}")
        print(f"[ERROR] Type: {type(e).__name__}, Details: {str(e)}")
        return {
            "statusCode": 500,
            "body": str(e)
        }

# S3 related code
def save_to_s3(stock_data, symbol):
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    if not s3_bucket:
        raise Exception("Please configure S3 bucket name environment variable S3_BUCKET_NAME")
    s3_key = f"stock-data/{symbol}/latest_data.json"
    print(f"[2] Preparing to store to S3: {s3_bucket}/{s3_key}")
    try:
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=json.dumps(stock_data, indent=2),
            ContentType='application/json'
        )
        print(f"[2] Data successfully overwritten to S3: {s3_key}")
    except Exception as e:
        print(f"[ERROR] S3 operation failed: {str(e)}")
        raise Exception(f"S3 error: {str(e)}")
    finally:
        print("[2] S3 data storage process finished")
    
    return s3_key

# Removed RDS related code as it's not needed for this Lambda.
# You will create a separate Lambda2 for that task.
