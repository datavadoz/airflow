""" This DAG crawls data from Google and Facebook ad library """
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from cps.crawl.google import fetch_google_ads
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


def create_bq_datasets(**kwargs):
    """ Create a set of datasets """
    bq_datasets = kwargs.get('bq_datasets')
    bq = BigQuery('default_bigquery')

    for dataset_name in bq_datasets:
        bq.create_dataset(dataset_name)
        print(f'Created {dataset_name} dataset')


def fetch_facebook_ads():
    pass


with DAG(
    'cps_mkt_crawl_ads',
    default_args=default_args,
    schedule_interval='0 8 * * *'
) as dag:
    t001 = EmptyOperator(task_id='start')
    t999 = EmptyOperator(task_id='end')

    t002 = PythonOperator(
        task_id='create_datasets',
        python_callable=create_bq_datasets,
        op_kwargs={
            'bq_datasets': [
                'cps_ads_google',
                'cps_ads_google_raw',
                'cps_ads_meta',
                'cps_ads_meta_raw'
            ]
        }
    )

    t003 = PythonOperator(
        task_id='fetch_google_ads',
        python_callable=fetch_google_ads
    )

    t004 = PythonOperator(
        task_id='fetch_facebook_ads',
        python_callable=fetch_facebook_ads
    )

    t001 >> t002 >> [t003, t004] >> t999
