import time
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from toolbox.gcp.bigquery import BigQuery, GSheetTable


local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
default_args = {
    'owner': 'Danh Vo',
    'description': "Monitor metrics then send notification via Telegram",
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 16, tzinfo=local_tz),
    'email': ['danhvo.uit@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


def create_external_table():
    bq = BigQuery('default_bigquery')
    bq.create_bq_table_from_gsheet_table(
        gsheet_table=GSheetTable(
            '14zV1me4r6dHQn6c7nBbpW549eumP9OdfVfUq3kH51uQ',
            'FB_Day',
            'cps_gsheet_bot_facebook.json'
        ),
        full_table_id='datavadoz-438714.cps_monitor_gsheet.facebook',
        recreate_if_exists=True
    )


def create_partitioned_table(**kwargs):
    execution_date = kwargs.get('execution_date').strftime('%Y-%m-%d')

    bq = BigQuery('default_bigquery')
    retries = 100

    while retries > 0:
        result = bq.run_query('datavadoz-438714.cps_monitor_gsheet.facebook')
        last_three_dates = [
            date.strftime('%Y-%m-%d')
            for date in result.to_dict(as_series=False)['date']
        ]

        print(f'Execution date: {execution_date}')
        print(f'Last three dates: {last_three_dates}')

        if execution_date in set(last_three_dates):
            break

        retries -= 1
        print(f'Remaining retries {retries}. Sleeping...')
        time.sleep(30)

    bq.create_partitioned_table(
        full_table_id='datavadoz-438714.cps_monitor_gsheet.facebook',
        partitioned_column='date'
    )


with DAG(
    'cps_mkt_monitor',
    default_args=default_args,
    schedule_interval='30 10 * * *'
) as dag:
    t001 = EmptyOperator(task_id='start')
    t999 = EmptyOperator(task_id='end')

    t002 = PythonOperator(
        task_id='create_external_table_facebook',
        python_callable=create_external_table
    )

    t003 = PythonOperator(
        task_id='create_partitioned_table_facebook',
        python_callable=create_partitioned_table
    )

    t001 >> t002 >> t003 >> t999
