import os
import time
from datetime import datetime, timedelta

import pendulum
import polars as pl
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

from toolbox.config import get_sql_folder
from toolbox.gcp.bigquery import BigQuery, GSheetTable

DMC3_LIST = [
    'ALL',
    'APPLE WATCH CTM',
    'ĐIỆN GIA ĐÌNH',
    'ĐỒNG HỒ',
    'IPAD CTM',
    'IPHONE CTM',
    'IPHONE CŨ',
    'LAPTOP CTM',
    'LOA CAO CẤP',
    'MACBOOK CTM',
    'NHẬP CŨ',
    'NUBIA CTM',
    'OPPO CTM',
    'PHỤ KIỆN APPLE',
    'PHỤ KIỆN CỦ CÁP',
    'PHỤ KIỆN DÁN MÀN HÌNH',
    'PHỤ KIỆN IT',
    'SAMSUNG CTM',
    'TAI NGHE CAO CẤP',
    'TECNO CTM',
    'THIẾT BỊ VĂN PHÒNG',
    'XIAOMI CTM',
]


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


def create_external_table(**kwargs):
    channel = kwargs.get('channel')
    if channel == 'facebook':
        gsheet_table = GSheetTable(
            '14zV1me4r6dHQn6c7nBbpW549eumP9OdfVfUq3kH51uQ',
            'FB_Day',
            'cps_gsheet_bot_facebook.json'
        )
    elif channel == 'google':
        gsheet_table = GSheetTable(
            '14zV1me4r6dHQn6c7nBbpW549eumP9OdfVfUq3kH51uQ',
            'GG_ADs_Day',
            'cps_gsheet_bot_google.json'
        )
    else:
        print(f'Unknown channel {channel}')
        exit(1)

    bq = BigQuery('default_bigquery')
    bq.create_bq_table_from_gsheet_table(
        gsheet_table=gsheet_table,
        full_table_id=f'datavadoz-438714.cps_monitor_gsheet.{channel}',
        recreate_if_exists=True
    )


def create_partitioned_table(**kwargs):
    execution_date = kwargs.get('execution_date')
    execution_date = execution_date.strftime('%Y-%m-%d')
    channel = kwargs.get('channel')
    full_table_id = f'datavadoz-438714.cps_monitor_gsheet.{channel}'

    bq = BigQuery('default_bigquery')

    sql_template_path = os.path.join(get_sql_folder(), 'sql_002.sql')
    with open(sql_template_path, 'r') as f:
        sql_template = f.read()

    sql_stmt = sql_template.format(
        full_table_id=full_table_id
    )

    retries = 100
    while retries > 0:
        result = bq.run_query(sql_stmt)
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
        full_table_id=full_table_id,
        partitioned_column='date'
    )


def gen_report(**kwargs):
    channel = kwargs.get('channel')
    execution_date = kwargs.get('execution_date')
    previous_date = execution_date - timedelta(days=1)
    execution_date = execution_date.strftime('%Y-%m-%d')
    previous_date = previous_date.strftime('%Y-%m-%d')

    if channel == 'google':
        print(f'Unimplemented for {channel}')
        return

    bq = BigQuery('default_bigquery')

    for dmc3 in DMC3_LIST:
        dmc3_condition = f"AND dmc3 = '{dmc3}'"
        if dmc3.upper() == 'ALL':
            dmc3_condition = ''

        sql_template_path = os.path.join(get_sql_folder(), 'sql_003.sql')
        with open(sql_template_path, 'r') as f:
            sql_template = f.read()

        sql_stmt = sql_template.format(
            today=execution_date,
            previous_date=previous_date,
            dmc3_condition=dmc3_condition
        )

        result = bq.run_query(sql_stmt)
        result = result.filter(
            pl.col('date') == datetime.strptime(execution_date, '%Y-%m-%d')
        )

        print(result)

        header = ''
        body = ''

        for row in result.rows(named=True):
            source = row['source']
            cost = row['total_cost']
            cpc = f'${row["cpc"]:.3f}' if row['cpc'] else 'N/A'
            cpa = f'${row["cpa"]:.3f}' if row['cpa'] else 'N/A'
            diff_cost = row['diff_cost']
            diff_cpc = f'{row["diff_cpc"]:.1f}%' if row['diff_cpc'] else 'N/A'
            diff_cpa = f'{row["diff_cpa"]:.1f}%' if row['diff_cpa'] else 'N/A'

            if source == 'all':
                header += f"=== Facebook Cost Report on *{execution_date}*\n" \
                          f">>> *{dmc3}*\n"
                header += f"- *Total*: ${cost:,} ({diff_cost:.1f}%) | " \
                          f"CPC: {cpc} ({diff_cpc}) | " \
                          f"CPA: {cpa} ({diff_cpa})\n"
            else:
                body += f"  *{source}*: ${cost:,} ({diff_cost:.1f}%)\n" \
                        f"  - CPC: {cpc} ({diff_cpc})\n" \
                        f"  - CPA: {cpa} ({diff_cpa})\n"

        msg = header + body
        if len(msg) == 0:
            print(f'DMC3 {dmc3} does not have data!')
            continue

        TelegramOperator(
            task_id='not_important',
            telegram_conn_id='tlg_prod_facebook',
            telegram_kwargs={'parse_mode': 'Markdown'},
            text=msg,
        ).execute(kwargs)


with DAG(
    'cps_mkt_monitor_cost',
    default_args=default_args,
    schedule_interval='45 10 * * *'
) as dag:
    t001 = EmptyOperator(task_id='start')
    t999 = EmptyOperator(task_id='end')

    t002 = PythonOperator(
        task_id='create_external_table_facebook',
        python_callable=create_external_table,
        op_kwargs={'channel': 'facebook'}
    )

    t003 = PythonOperator(
        task_id='create_partitioned_table_facebook',
        python_callable=create_partitioned_table,
        op_kwargs={'channel': 'facebook'}
    )

    t004 = PythonOperator(
        task_id='generate_report_facebook',
        python_callable=gen_report,
        op_kwargs={'channel': 'facebook'}
    )

    t102 = PythonOperator(
        task_id='create_external_table_google',
        python_callable=create_external_table,
        op_kwargs={'channel': 'google'}
    )

    t103 = PythonOperator(
        task_id='create_partitioned_table_google',
        python_callable=create_partitioned_table,
        op_kwargs={'channel': 'google'}
    )

    t104 = PythonOperator(
        task_id='generate_report_google',
        python_callable=gen_report,
        op_kwargs={'channel': 'google'}
    )

    t001 >> t002 >> t003 >> t004 >> t999
    t001 >> t102 >> t103 >> t104 >> t999
