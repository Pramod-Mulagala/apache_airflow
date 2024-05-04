import pandas as pd
import re
import boto3
from io import StringIO
from s3_details import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME

def data_cleaner():
    # Initialize S3 client
    s3 = boto3.client('s3', 
                      aws_access_key_id=AWS_ACCESS_KEY_ID, 
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    # Read CSV from S3
    file_key = 'raw_store_transactions.csv'
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    df = pd.read_csv(obj['Body'])
    
    # Data cleaning functions
    def clean_store_location(st_loc):
        return re.sub(r'[^\w\s]', '', st_loc).strip()

    def clean_product_id(pd_id):
        matches = re.findall(r'\d+', pd_id)
        if matches:
            return matches[0]
        return pd_id

    def remove_dollar(amount):
        return float(amount.replace('$', ''))

    # Apply data cleaning functions
    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(lambda x: clean_store_location(x))
    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(lambda x: clean_product_id(x))

    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(lambda x: remove_dollar(x))

    # Write cleaned data to CSV
    cleaned_csv_buffer = StringIO()
    df.to_csv(cleaned_csv_buffer, index=False)
    
    # Write cleaned CSV to S3
    cleaned_file_key = 'clean_store_transactions.csv'
    s3.put_object(Bucket=BUCKET_NAME, Key=cleaned_file_key, Body=cleaned_csv_buffer.getvalue())

data_cleaner()

