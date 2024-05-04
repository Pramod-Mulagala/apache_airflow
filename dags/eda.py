import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import boto3
from io import BytesIO
from io import StringIO
from s3_details import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME

def eda():
    # Initialize S3 client
    s3 = boto3.client('s3', 
                      aws_access_key_id=AWS_ACCESS_KEY_ID, 
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    # Read CSV from S3
    file_key = 'clean_store_transactions.csv'
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    data = pd.read_csv(obj['Body'])

    # Convert 'Date' to datetime format and add a 'Year_Week' column for trend analysis
    data['Date'] = pd.to_datetime(data['Date'])
    data['Year_Week'] = data['Date'].dt.to_period('W').astype(str)

    # Distribution Analysis
    mrp_desc = data['MRP'].describe()
    cp_desc = data['CP'].describe()
    discount_desc = data['DISCOUNT'].describe()
    sp_desc = data['SP'].describe()

    # Additional statistics
    mrp_skewness = data['MRP'].skew()
    cp_skewness = data['CP'].skew()
    discount_skewness = data['DISCOUNT'].skew()
    sp_skewness = data['SP'].skew()

    mrp_kurtosis = data['MRP'].kurtosis()
    cp_kurtosis = data['CP'].kurtosis()
    discount_kurtosis = data['DISCOUNT'].kurtosis()
    sp_kurtosis = data['SP'].kurtosis()

    # Save summary statistics to a DataFrame
    summary_stats = pd.DataFrame({
        'Statistic': ['Mean', 'Std', 'Min', '25%', '50%', '75%', 'Max', 'Skewness', 'Kurtosis'],
        'MRP': [mrp_desc['mean'], mrp_desc['std'], mrp_desc['min'], mrp_desc['25%'], 
                mrp_desc['50%'], mrp_desc['75%'], mrp_desc['max'], mrp_skewness, mrp_kurtosis],
        'CP': [cp_desc['mean'], cp_desc['std'], cp_desc['min'], cp_desc['25%'], 
               cp_desc['50%'], cp_desc['75%'], cp_desc['max'], cp_skewness, cp_kurtosis],
        'DISCOUNT': [discount_desc['mean'], discount_desc['std'], discount_desc['min'], discount_desc['25%'], 
                     discount_desc['50%'], discount_desc['75%'], discount_desc['max'], discount_skewness, discount_kurtosis],
        'SP': [sp_desc['mean'], sp_desc['std'], sp_desc['min'], sp_desc['25%'], 
               sp_desc['50%'], sp_desc['75%'], sp_desc['max'], sp_skewness, sp_kurtosis]
    })
        
        
   # Upload summary statistics CSV to S3
    summary_stats_buffer = StringIO()
    summary_stats.to_csv(summary_stats_buffer, index=False)
    cleaned_file_key = 'eda_summary_stats.csv'
    s3.put_object(Bucket=BUCKET_NAME, Key=cleaned_file_key, Body=summary_stats_buffer.getvalue())


eda()
