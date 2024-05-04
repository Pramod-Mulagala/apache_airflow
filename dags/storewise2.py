import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import boto3
from io import StringIO
from s3_details import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME

def store_wise():
    # Initialize S3 client
    s3 = boto3.client('s3', 
                      aws_access_key_id=AWS_ACCESS_KEY_ID, 
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    # Read CSV from S3
    file_key = 'clean_store_transactions.csv'
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    data = pd.read_csv(obj['Body'])

    # Compute metrics for store-wise analysis
    store_metrics = data.groupby('STORE_ID').agg(
        Total_Sales=pd.NamedAgg(column='SP', aggfunc='sum'),
        Average_Discount=pd.NamedAgg(column='DISCOUNT', aggfunc='mean'),
    ).reset_index()

    # Identify the store with the highest and lowest sales
    highest_sales_store = store_metrics.loc[store_metrics['Total_Sales'].idxmax()]
    lowest_sales_store = store_metrics.loc[store_metrics['Total_Sales'].idxmin()]

    # Determine medians for grouping
    sales_median = store_metrics['Total_Sales'].median()
    discount_median = store_metrics['Average_Discount'].median()

    # Assign groups based on performance
    store_metrics['Sales_Group'] = store_metrics['Total_Sales'].apply(lambda x: 'High Sales' if x >= sales_median else 'Low Sales')
    store_metrics['Discount_Group'] = store_metrics['Average_Discount'].apply(lambda x: 'High Discount' if x >= discount_median else 'Low Discount')

    # Prepare suggestions data for CSV
    suggestions = {
        'High Sales': "Optimize inventory & explore new markets.",
        'Low Sales': "Boost marketing efforts & review product mix."
    }
    suggestions_df = pd.DataFrame(suggestions.items(), columns=['Sales_Group', 'Suggestion'])

    # Merge store-wise metrics with suggestions
    combined_data = store_metrics.merge(suggestions_df, on='Sales_Group')
        
    # Upload summary statistics CSV to S3
    combined_data_buffer = StringIO()
    combined_data.to_csv(combined_data_buffer, index=False)
    cleaned_file_key = 'store_wise.csv'
    s3.put_object(Bucket=BUCKET_NAME, Key=cleaned_file_key, Body=combined_data_buffer.getvalue())

    # Visualization adjustments
    # For the sake of simplicity, we'll focus on the Sales Performance Groups visualization and suggestions

    # Prepare the data for Sales Performance Groups
    sales_group_counts = store_metrics['Sales_Group'].value_counts().reset_index()
    sales_group_counts.columns = ['Sales_Group', 'Number_of_Stores']  # Correcting column names

    plt.figure(figsize=(10, 6))
    sns.barplot(x='Sales_Group', y='Number_of_Stores', data=sales_group_counts, palette='coolwarm')
    plt.title('Stores by Sales Performance Group')

    # Highlighting the store with highest and lowest sales
    plt.annotate(f'Highest Sales: {highest_sales_store["STORE_ID"]}', xy=(0, 0), xytext=(0.5, -0.25),
                xycoords='axes fraction', textcoords='axes fraction',
                arrowprops=dict(facecolor='green', shrink=0.05), fontsize=10, color='green', ha='center')

    plt.annotate(f'Lowest Sales: {lowest_sales_store["STORE_ID"]}', xy=(1, 0), xytext=(0.5, -0.5),
                xycoords='axes fraction', textcoords='axes fraction',
                arrowprops=dict(facecolor='red', shrink=0.05), fontsize=10, color='red', ha='center')

    plt.ylabel('Number of Stores')
    plt.tight_layout()
    plt.show()


store_wise()
