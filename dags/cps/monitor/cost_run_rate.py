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


NOTIFY_TEMPLATE = """=== RUN-RATE: *{dmc3}*
- % Đạt doanh số: {sales}
- % MoM: {mom}
- Ngân sách (Actual: {actual_budget} | Plan: {actual_budget}) | actual/plan: {actual_vs_plan_budget})
- FB Catalog % actual/plan: {actual_vs_plan_fb_catalog}
- FB % actual/plan: {actual_vs_plan_fb}
- GG % actual/plan: {actual_vs_plan_gg}
- TT % actual/plan: {actual_vs_plan_tt}
- Dynamic % actual/plan: {actual_vs_plan_dynamic}
- Criteo RE % actual/plan: {actual_vs_plan_criteo_re}
- Criteo NEW % actual/plan: {actual_vs_plan_criteo_new}
"""

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
default_args = {
    'owner': 'Danh Vo',
    'description': "Monitor metrics then send notification via Telegram",
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 24, tzinfo=local_tz),
    'email': ['danhvo.uit@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


def create_external_table():
    bq = BigQuery('default_bigquery')
    bq.create_bq_table_from_gsheet_table(
        gsheet_table=GSheetTable(
            sheet_id='14zV1me4r6dHQn6c7nBbpW549eumP9OdfVfUq3kH51uQ',
            tab_name='Cost runrate!A2:AF71',
            schema_name='cps_gsheet_bot_cost_run_rate.json'
        ),
        full_table_id='datavadoz-438714.cps_monitor_gsheet.cost_run_rate',
        recreate_if_exists=True
    )


def notify(**kwargs):
    sql_template_path = os.path.join(get_sql_folder(), 'sql_004.sql')
    with open(sql_template_path, 'r') as f:
        sql_stmt = f.read()

    bq = BigQuery('default_bigquery')
    result = bq.run_query(sql_stmt)

    for row in result.rows(named=True):
        dmc3 = row['dmc3'] \
            if row['dmc3'] else 'N/A'
        sales = f"{row['sales']}%" \
            if row['sales'] else 'N/A'
        mom = f"{row['mom']}%" \
            if row['mom'] else 'N/A'
        actual_budget = f"{row['actual_budget_perc']}%" \
            if row['actual_budget_perc'] else 'N/A'
        plan_budget = f"{row['plan_budget_perc']}%" \
            if row['plan_budget_perc'] else 'N/A'
        actual_vs_plan_budget = f"{row['actual_vs_plan_budget']}%" \
            if row['actual_vs_plan_budget'] else 'N/A'
        actual_vs_plan_fb_catalog = f"{row['actual_vs_plan_fb_catalog']}%" \
            if row['actual_vs_plan_fb_catalog'] else 'N/A'
        actual_vs_plan_fb = f"{row['actual_vs_plan_fb']}%" \
            if row['actual_vs_plan_fb'] else 'N/A'
        actual_vs_plan_gg = f"{row['actual_vs_plan_gg']}%" \
            if row['actual_vs_plan_gg'] else 'N/A'
        actual_vs_plan_tt = f"{row['actual_vs_plan_tt']}%" \
            if row['actual_vs_plan_tt'] else 'N/A'
        actual_vs_plan_dynamic = f"{row['actual_vs_plan_dynamic']}%" \
            if row['actual_vs_plan_dynamic'] else 'N/A'
        actual_vs_plan_criteo_re = f"{row['actual_vs_plan_criteo_re']}%" \
            if row['actual_vs_plan_criteo_re'] else 'N/A'
        actual_vs_plan_criteo_new = f"{row['actual_vs_plan_criteo_new']}%" \
            if row['actual_vs_plan_criteo_new'] else 'N/A'

        msg = NOTIFY_TEMPLATE.format(
            dmc3=dmc3,
            sales=sales,
            mom=mom,
            actual_budget=actual_budget,
            plan_budget=plan_budget,
            actual_vs_plan_budget=actual_vs_plan_budget,
            actual_vs_plan_fb_catalog=actual_vs_plan_fb_catalog,
            actual_vs_plan_fb=actual_vs_plan_fb,
            actual_vs_plan_gg=actual_vs_plan_gg,
            actual_vs_plan_tt=actual_vs_plan_tt,
            actual_vs_plan_dynamic=actual_vs_plan_dynamic,
            actual_vs_plan_criteo_re=actual_vs_plan_criteo_re,
            actual_vs_plan_criteo_new=actual_vs_plan_criteo_new
        )

        TelegramOperator(
            task_id='not_important',
            telegram_conn_id='tlg_prod_run_rate',
            telegram_kwargs={'parse_mode': 'Markdown'},
            text=msg,
        ).execute(kwargs)

        time.sleep(3)


with DAG(
    'cps_mkt_monitor_cost_run_rate',
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
