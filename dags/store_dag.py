from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.email import EmailOperator
from s3_details import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME

from datacleaner_s3 import data_cleaner
from eda import eda
from locwise2 import location_wise
from storewise2 import store_wise
from model import predictions
from visualize import visualize
from send_email import send_email

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 12, 9),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('store_dag',
         default_args=default_args,
         schedule_interval='@once',
         catchup=True) as dag:

    t1 = S3KeySensor(
    task_id='check_file_exists',
    bucket_key='s3://storesales-airflow/raw_store_transactions.csv',
    aws_conn_id='aws_default',  # Remove the aws_conn_id parameter
    poke_interval=120
    )

    t2 = PythonOperator(
        task_id='clean_raw_csv',
        python_callable=data_cleaner
    )

    t3 = PythonOperator(
        task_id='perform_eda',
        python_callable=eda
    )

    t4 = PythonOperator(
        task_id='location_wise_insights',
        python_callable=location_wise
    )

    t5 = PythonOperator(
        task_id='store_wise_insights',
        python_callable=store_wise
    )
    
    t6 = PythonOperator(
        task_id='predictions',
        python_callable=predictions
    )
    
    t7 = PythonOperator(
    task_id='create_a_dashboard',
    python_callable=visualize
    )

    t8 = PythonOperator(
    task_id='download_file_and_send_email',
    python_callable=send_email,
    provide_context=True,  # Pass context to the function
    dag=dag  # Assuming dag is your DAG object
    )

    t1 >> t2 >> t3 
    t3 >> t4
    t3 >> t5
    t4 >> t6
    t5 >> t6
    t6 >> t7
    t7 >> t8


