import json 
import os 
import boto3 
import mysql.connector 
from mysql.connector import Error 
from datetime import datetime 

# Make sure to include mysql-connector-python in your deployment package or Lambda Layer 
# ODS 到ADS

def save_to_rds(stock_data, symbol): 
    """ 
    Connects to the RDS database using environment variables and inserts stock data. 
    """ 
    # Get database connection details from environment variables 
    db_config = { 
        'host': os.getenv('RDS_HOST'), 
        'user': os.getenv('RDS_USER'), 
        'password': os.getenv('RDS_PASSWORD'),  
        'database': os.getenv('RDS_DATABASE'), 
        'port': int(os.getenv('RDS_PORT', 3306)) 
    } 

    # Log to verify environment variables are loaded correctly 
    print(f"[3] Debugging: Trying to connect to RDS with user: {db_config['user']}, host: {db_config['host']}") 

    connection = None 
    cursor = None 
    try: 
        start_time = datetime.now()
        print(f"[3] Attempting RDS connection to host: {db_config['host']}") 
        connection = mysql.connector.connect( 
            **db_config, 
            connect_timeout=10 
        ) 
        if connection.is_connected(): 
            print(f"[3] RDS connection successful. (took: {(datetime.now() - start_time).total_seconds():.2f}s)") 
            
            cursor = connection.cursor() 
            for date_str, values in stock_data.items(): 
                date = datetime.strptime(date_str, '%Y-%m-%d').date() 
                print(f"[3] Inserting record for {date}") 
                insert_sql = """ 
                INSERT IGNORE INTO stock_prices 
                (symbol, date, open, high, low, close, volume) 
                VALUES (%s, %s, %s, %s, %s, %s, %s) 
                """ 
                cursor.execute(insert_sql, ( 
                    symbol, date, 
                    values['1. open'], values['2. high'], 
                    values['3. low'], values['4. close'], 
                    values['5. volume'] 
                )) 
            connection.commit() 
            print(f"[3] Successfully committed {cursor.rowcount} records")

    except Error as e: 
        print(f"[ERROR] RDS operation failed: {str(e)}") 
        if "Authentication plugin" in str(e): 
            print("[ERROR] Hint: Check your database user/password.") 
        elif "Unknown database" in str(e): 
            print("[ERROR] Hint: Check if the database name in environment variables is correct.") 
        elif "Can't connect to MySQL server" in str(e): 
            print("[ERROR] Hint: Check VPC security group, subnet, and host environment variable.") 
        
        if connection: 
            connection.rollback() 
        raise Exception(f"Database error: {str(e)}") 
    except Exception as e: 
        print(f"[ERROR] An unexpected error occurred: {str(e)}") 
        raise Exception(f"Connection error: {str(e)}") 
    finally: 
        if cursor: 
            cursor.close() 
            print("[3] Cursor closed.") 
        if connection and connection.is_connected(): 
            connection.close() 
            print("[3] Connection closed.") 
        print("[3] Data storage process finished.") 

def lambda_handler(event, context): 
    print(f"=== Lambda2 execution started (RequestId: {context.aws_request_id}) ===") 

    # 1. Get S3 bucket and key from the event sent by Step Functions
    try:
        bucket = event['s3_bucket']
        key = event['s3_key']
        print(f"[1] Event received for bucket: {bucket}, key: {key}")
    except KeyError as e:
        print(f"[ERROR] Invalid event format from Step Functions: Missing key {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid event format. Missing bucket or key information.')
        }

    # 2. Read JSON data from S3
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        print(f"[2] Successfully read data from S3. Data size: {len(data)}")
    except Exception as e:
        print(f"[ERROR] Failed to read data from S3: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to read data from S3.')
        }

    # 3. Save data to RDS
    try:
        save_to_rds(data, "MSFT")
        return { 
            'statusCode': 200, 
            'body': json.dumps('Data successfully written to RDS.') 
        } 
    except Exception as e: 
        print(f"[ERROR] RDS operation failed in lambda_handler: {str(e)}") 
        return { 
            'statusCode': 500, 
            'body': json.dumps(f'Failed to write data to RDS: {str(e)}') 
        } 
