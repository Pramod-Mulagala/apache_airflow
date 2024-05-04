from airflow.operators.email_operator import EmailOperator
from airflow.hooks.S3_hook import S3Hook
from s3_details import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME

def send_email(**kwargs):
    s3_conn_id = 'aws_default'  # The connection ID of your AWS S3 connection
    bucket_name = BUCKET_NAME  # The name of your S3 bucket
    s3_key = 'dashboard.pdf'  # The key (path) of the file on S3

    # Download file from S3
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    local_path = f'/usr/local/airflow/{s3_key}'
    s3_hook.download_file(bucket_name=bucket_name, key=s3_key, local_path=local_path)

    # Attach downloaded file to the email
    t8 = EmailOperator(
        task_id='send_email',
        to='example@example.com',
        subject='Daily report generated',
        html_content="<h1>Congratulations! Your Store Reports are ready.</h1>",
        files=[local_path],
        dag=kwargs['dag']
    )