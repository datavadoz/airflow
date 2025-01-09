import os
import time
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

from toolbox.config import get_sql_folder
from toolbox.gcp.bigquery import BigQuery, GSheetTable

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
default_args = {
    'owner': 'Danh Vo',
    'description': "Monitor metrics then send notification via Telegram",
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 8, tzinfo=local_tz),
    'email': ['danhvo.uit@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


def create_external_table():
    bq = BigQuery('default_bigquery')
    bq.create_bq_table_from_gsheet_table(
        gsheet_table=GSheetTable(
            sheet_id='14zV1me4r6dHQn6c7nBbpW549eumP9OdfVfUq3kH51uQ',
            tab_name='Cost runrate!AF76:AQ100',
            schema_name='cps_gsheet_bot_traffic.json'
        ),
        full_table_id='datavadoz-438714.cps_monitor_gsheet.traffic',
        recreate_if_exists=True
    )


with DAG(
    'cps_mkt_monitor_traffic',
    default_args=default_args,
    schedule_interval='45 10 * * *'
) as dag:
    t001 = EmptyOperator(task_id='start')
    t999 = EmptyOperator(task_id='end')

    t002 = PythonOperator(
        task_id='create_external_table',
        python_callable=create_external_table
    )

    t001 >> t002 >> t999
