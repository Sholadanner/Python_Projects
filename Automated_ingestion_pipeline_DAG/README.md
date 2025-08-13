from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.utils.dates import days_ago
import requests

BUCKET_NAME = 'covid_data_ingestion'
CSV_URL = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'
LOCAL_FILE = '/tmp/owid_covid_data.csv'
GCS_FILE = 'covid/owid_covid_data.csv'
BQ_TABLE = 'emerald-handler-463113-d4.covid_data.owid_daily'

def download_covid_data():
    response = requests.get(CSV_URL)
    with open(LOCAL_FILE, 'wb') as f:
        f.write(response.content)

def upload_to_gcs():
    from google.cloud import storage
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    blob = bucket.blob(GCS_FILE)
    blob.upload_from_filename(LOCAL_FILE)

with DAG(
    dag_id='covid_data_ingestion',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=["covid", "ingestion"],
) as dag:

    download_task = PythonOperator(
        task_id='download_csv',
        python_callable=download_covid_data,
    )

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
    )

    load_bq_task = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=[GCS_FILE],
        destination_project_dataset_table=BQ_TABLE,
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    download_task >> upload_task >> load_bq_task
