import os
from datetime import datetime

import polars as pl
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

from toolbox.config import get_sql_folder
from toolbox.gcp.bigquery import BigQuery, GSheetTable

HEAD_MSG_TEMPLATE = """Tổng: (TM: *{summary_tm}* | MoM: *{summary_mom}*)
Tỉnh: (TM: *{province_tm}* | MoM: *{province_mom}*)
Phố: (TM: *{city_tm}* | MoM: *{city_mom}*)
"""

BODY_MSG_TEMPLATE = "{channel}: (TM: *{tm}* | MoM: *{mom}*)\n"

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


def send_tlg_msg(msg, **kwargs):
    TelegramOperator(
        task_id='not_important',
        telegram_conn_id='tlg_prod_session',
        telegram_kwargs={'parse_mode': 'Markdown'},
        text=msg,
    ).execute(kwargs)


def notify(**kwargs):
    execution_date = kwargs.get('execution_date').strftime('%Y-%m-%d')

    sql_template_path = os.path.join(get_sql_folder(), 'sql_007.sql')
    with open(sql_template_path, 'r') as f:
        sql_stmt = f.read()

    bq = BigQuery('default_bigquery')
    result = bq.run_query(sql_stmt)

    msg = f'Paid channels *{execution_date}*\n'
    head_result = result.filter(pl.col('channel') == 'Paid channels')
    for line in head_result.rows(named=True):
        summary_tm = \
            f"{round(line['summary_tm'], 2):,}" if line['summary_tm'] else 'N/A'
        summary_mom = \
            f"{round(line['summary_mom'], 2):,}" if line['summary_mom'] else 'N/A'
        province_tm = \
            f"{round(line['province_tm'], 2):,}" if line['province_tm'] else 'N/A'
        province_mom = \
            f"{round(line['province_mom'], 2):,}" if line['province_mom'] else 'N/A'
        city_tm = \
            f"{round(line['city_tm'], 2):,}" if line['city_tm'] else 'N/A'
        city_mom = \
            f"{round(line['city_mom'], 2):,}" if line['city_mom'] else 'N/A'

        msg += HEAD_MSG_TEMPLATE.format(
            summary_tm=summary_tm,
            summary_mom=summary_mom,
            province_tm=province_tm,
            province_mom=province_mom,
            city_tm=city_tm,
            city_mom=city_mom
        )
    send_tlg_msg(msg, **kwargs)

    msg = f'=== Session Tổng *{execution_date}*\n'
    summary_result = result.filter(pl.col('summary_tm') > 0)
    for line in summary_result.rows(named=True):
        channel = line['channel']
        tm = f"{round(line['summary_tm'], 2):,}" if line['summary_tm'] else 'N/A'
        mom = f"{round(line['summary_mom'], 2):,}" if line['summary_mom'] else 'N/A'
        msg += BODY_MSG_TEMPLATE.format(channel=channel, tm=tm, mom=mom)
    send_tlg_msg(msg, **kwargs)

    msg = f'=== Session Tỉnh *{execution_date}*\n'
    province_result = result.filter(pl.col('province_tm') > 0)
    for line in province_result.rows(named=True):
        channel = line['channel']
        tm = f"{round(line['province_tm'], 2):,}" if line['province_tm'] else 'N/A'
        mom = f"{round(line['province_mom'], 2):,}" if line['province_mom'] else 'N/A'
        msg += BODY_MSG_TEMPLATE.format(channel=channel, tm=tm, mom=mom)
    send_tlg_msg(msg, **kwargs)

    msg = f'=== Session Phố *{execution_date}*\n'
    city_result = result.filter(pl.col('city_tm') > 0)
    for line in city_result.rows(named=True):
        channel = line['channel']
        tm = f"{round(line['city_tm'], 2):,}" if line['city_tm'] else 'N/A'
        mom = f"{round(line['city_mom'], 2):,}" if line['city_mom'] else 'N/A'
        msg += BODY_MSG_TEMPLATE.format(channel=channel, tm=tm, mom=mom)
    send_tlg_msg(msg, **kwargs)


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

    t003 = PythonOperator(
        task_id='notify',
        python_callable=notify
    )

    t001 >> t002 >> t003 >> t999
