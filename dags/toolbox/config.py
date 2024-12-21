import os

from airflow import configuration


def get_dag_folder():
    return configuration.conf.get('core', 'dags_folder')


def get_schema_folder():
    dag_folder = get_dag_folder()
    return os.path.join(dag_folder, 'schema')


def get_sql_folder():
    dag_folder = get_dag_folder()
    return os.path.join(dag_folder, 'sql')
