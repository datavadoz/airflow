from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator

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

with DAG(
    'cps_mkt_crawl_ads',
    default_args=default_args,
    schedule_interval='0 8 * * *'
) as dag:
    t001 = EmptyOperator(task_id='start')
    t999 = EmptyOperator(task_id='end')

    t001 >> t999
