import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import boto3
from io import StringIO
from s3_details import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME

def location_wise():
    # Initialize S3 client
    s3 = boto3.client('s3', 
                      aws_access_key_id=AWS_ACCESS_KEY_ID, 
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    # Read CSV from S3
    file_key = 'clean_store_transactions.csv'
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    data = pd.read_csv(obj['Body'])

    # Calculate total sales and average discount by location
    location_sales_discount = data.groupby('STORE_LOCATION').agg(
        Total_Sales=pd.NamedAgg(column='SP', aggfunc='sum'),
        Average_Discount=pd.NamedAgg(column='DISCOUNT', aggfunc='mean')
    ).reset_index()

    # Identify top and bottom categories by sales in each location
    top_categories = data.groupby(['STORE_LOCATION', 'PRODUCT_CATEGORY'])['SP'].sum().reset_index()
    top_categories = top_categories.sort_values(['STORE_LOCATION', 'SP'], ascending=[True, False])

    # Combine data into a single DataFrame
    combined_data = location_sales_discount.merge(top_categories, on='STORE_LOCATION')
        
    # Upload summary statistics CSV to S3
    combined_data_buffer = StringIO()
    combined_data.to_csv(combined_data_buffer, index=False)
    cleaned_file_key = 'location_wise.csv'
    s3.put_object(Bucket=BUCKET_NAME, Key=cleaned_file_key, Body=combined_data_buffer.getvalue())

    # Plotting code
    # Adjusted Visualization for Total Sales and Average Discount by Location with Suggestions
    plt.figure(figsize=(14, 7))
    ax1 = sns.barplot(x='STORE_LOCATION', y='Total_Sales', data=location_sales_discount, palette='viridis')
    ax1.set_title('Total Sales and Average Discount by Location with Suggestions')
    ax1.set_ylabel('Total Sales ($)')
    ax1.set_xlabel('')
    ax1.tick_params(axis='x', rotation=45)
    ax2 = ax1.twinx()
    sns.lineplot(x='STORE_LOCATION', y='Average_Discount', data=location_sales_discount, marker='o', color='red', label='Average Discount')
    ax2.set_ylabel('Average Discount (%)')
    plt.legend(loc='upper left')

    # Adding text annotations with suggestions
    for i, (location, row) in enumerate(location_sales_discount.iterrows()):
        suggestion = "Review discount strategy" if row['Average_Discount'] > location_sales_discount['Average_Discount'].mean() else "Maintain/Increase discount"
        ax1.text(i, row['Total_Sales'], suggestion, color='green', rotation=45, ha="center")

    plt.tight_layout()
    plt.show()

    # Visualization for Top & Bottom Categories in a Specific Location with Suggestions
    # Example location, assuming 'New York' is in the dataset
    example_location = 'New York'
    top_categories_example = top_categories[top_categories['STORE_LOCATION'] == example_location].head(3)  # Top 3 categories
    bottom_categories_example = top_categories[top_categories['STORE_LOCATION'] == example_location].tail(3)  # Bottom 3 categories
    category_data = pd.concat([top_categories_example, bottom_categories_example])

    plt.figure(figsize=(12, 7))
    sns.barplot(x='PRODUCT_CATEGORY', y='SP', data=category_data, palette='coolwarm')
    plt.title(f'Top & Bottom Performing Categories in {example_location} with Suggestions')
    plt.ylabel('Sales ($)')
    plt.xlabel('Product Category')

    # Adding text annotations with suggestions
    for i, row in enumerate(category_data.itertuples()):
        suggestion = "Expand inventory" if row.PRODUCT_CATEGORY in top_categories_example['PRODUCT_CATEGORY'].values else "Review strategy"
        plt.text(i, row.SP, suggestion, color='blue', rotation=45, ha="center")

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


location_wise()
