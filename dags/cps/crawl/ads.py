from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from toolbox.gcp.bigquery import BigQuery

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
default_args = {
    'owner': 'Danh Vo',
    'description': "Crawl Google & Facebook advertisement data",
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 4, tzinfo=local_tz),
    'email': ['danhvo.uit@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


def create_bq_dataset(**kwargs):
    bq_datasets = kwargs.get('bq_datasets')
    bq = BigQuery('default_bigquery')

    for dataset_name in bq_datasets:
        bq.create_dataset(dataset_name)
        print(f'Created {dataset_name} dataset')


with DAG(
    'cps_mkt_crawl_ads',
    default_args=default_args,
    schedule_interval='0 8 * * *'
) as dag:
    t001 = EmptyOperator(task_id='start')
    t999 = EmptyOperator(task_id='end')

    t002 = PythonOperator(
        task_id='create_datasets',
        python_callable=create_bq_dataset,
        op_kwargs={
            'bq_datasets': [
                'cps_ads_google',
                'cps_ads_google_history',
                'cps_ads_meta',
                'cps_ads_meta_history'
            ]
        }
    )

    t001 >> t002 >> t999
