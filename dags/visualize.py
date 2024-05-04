# Import necessary libraries
import dash
from dash import dcc
from dash import html
import pandas as pd
import plotly.express as px
import boto3
from io import BytesIO
from s3_details import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME
import pdfkit
import time

def visualize():
    # Initialize the Dash app
    app = dash.Dash(__name__)

    # Setup AWS S3 client
    #s3 = boto3.client('s3', region_name='us-east-1')  # specify your region
    s3 = boto3.client('s3', 
                        aws_access_key_id=AWS_ACCESS_KEY_ID, 
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Function to load data from S3
    def load_data_from_s3(bucket, key):
        response = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(response['Body'].read()))
    
    def upload_file_to_s3(bucket, file_path, object_name):
        with open(file_path, 'rb') as f:
            s3.upload_fileobj(f, bucket, object_name)

    # Load the datasets
    bucket_name = BUCKET_NAME  # Replace with your S3 bucket name
    store_transactions = load_data_from_s3(bucket_name, 'clean_store_transactions.csv')
    summary_stats = load_data_from_s3(bucket_name, 'eda_summary_stats.csv')
    location_wise = load_data_from_s3(bucket_name, 'location_wise.csv')
    store_wise = load_data_from_s3(bucket_name, 'store_wise.csv')

    # Prepare the store transactions data for plotting
    store_transactions['Date'] = pd.to_datetime(store_transactions['Date'])  # Ensure 'Date' is datetime type
    store_transactions_summary = store_transactions.groupby('Date').agg({'SP': 'sum'}).reset_index()

    # Define the layout of the dashboard
    app.layout = html.Div([
        html.H1('Store Performance Dashboard', style={'text-align': 'center'}),
        html.Div([
            dcc.Graph(
                id='transactions-plot',
                figure=px.bar(store_transactions_summary, x='Date', y='SP', title='Total Selling Price by Date')
            ),
        ], style={'width': '50%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(
                id='summary-stats-table',
                figure={
                    'data': [{'type': 'table',
                            'header': {'values': summary_stats.columns.tolist()},
                            'cells': {'values': summary_stats.transpose().values.tolist()}}]
                },
                config={'displayModeBar': False}
            ),
        ], style={'width': '50%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(
                id='sales-by-location',
                figure=px.bar(location_wise, x='STORE_LOCATION', y='Total_Sales', title='Total Sales by Location')
            ),
        ], style={'width': '50%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(
                id='sales-by-store',
                figure=px.bar(store_wise, x='STORE_ID', y='Total_Sales', title='Selling Price by Store')
            ),
        ], style={'width': '50%', 'display': 'inline-block'})
    ])
    
    @app.server.route('/download-pdf')
    def download_pdf():
        config = pdfkit.configuration(wkhtmltopdf='/usr/local/bin/wkhtmltopdf')
        pdfkit.from_url('http://localhost:8050', 'dashboard.pdf', configuration=config)
        time.sleep(5)
        upload_file_to_s3(bucket_name, 'dashboard.pdf', 'dashboard.pdf')
        print("File generated")
        return 'PDF has been generated and uploaded successfully.'
    # Run the app
    if __name__ == '__main__':
        app.run_server(debug=True)
        
visualize()