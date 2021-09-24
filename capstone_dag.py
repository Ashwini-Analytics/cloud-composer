# dag file 
#import libraries
from airflow import DAG
import pandas
from datetime import timedelta, datetime
from airflow.operators import python_operator
from google.cloud import storage
from google.cloud import bigquery
import json
import ast
from pandas.io.json import json_normalize
import gcsfs
from google.cloud import storage
#from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators import dummy_operator


YESTERDAY = datetime.now() - timedelta(days=1)
DAG_NAME = "capstone_dag"

#default parameters of the dag
default_args = {
    'owner': 'capstone1-project',
    'depends_on_past': False,
    'start_date': YESTERDAY ,
    'email': ['ragitashwiniwork@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

#This dag parameters will be applied to all the dags
dag = DAG(
    dag_id="capstone_dag",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    description="capstone_dag",
    max_active_runs=5,
)

#This method pulls the data from GCS bucket and performs the data processing on the data and then upload the data to gcs bucket.
def data_processing(self):
    fs = gcsfs.GCSFileSystem(project='capstone1-project-326220')
    with fs.open('gs://currency-bucket-egen/capstone_data.csv') as f:
        df = pandas.read_csv(f)
    rates = df['rates']
    key = []
    value = []
    for j in rates:
        i = ast.literal_eval(j)
        key.append(list(i.keys())[0])
        value.append(list(i.values())[0])
    df['country'] = key
    df['exchg'] = value
    df = df.drop("rates", axis=1)
    df.to_csv('gs://currency-bucket-egen/updated_data.csv')
    

start = dummy_operator.DummyOperator(task_id="start", dag=dag)
end = dummy_operator.DummyOperator(task_id="end", dag=dag)

data_processing = python_operator.PythonOperator(
    task_id='data_processing',
    python_callable=data_processing,
    dag=dag
)
#loads the datat from gcs to big Query.
load_csv = GoogleCloudStorageToBigQueryOperator(
    task_id='load_csv',
    bucket='currency-bucket-egen',
    source_objects=['updated_data.csv'],
    destination_project_dataset_table='capstone1-project-326220.currency_rates.rate',
    schema_fields=[
        {'name': 'success', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'base', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'exchg', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

#This is how the dag will work.
start >> data_processing >> load_csv >> end





