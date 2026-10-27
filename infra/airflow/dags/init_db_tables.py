from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="init_db_tables",
    start_date=datetime(2026, 3, 25),
    schedule=None,
    catchup=False,
    tags=["db", "init"],
    template_searchpath=["/opt/airflow/db/ddl"],
) as dag:

    create_raw_tables = PostgresOperator(
        task_id="create_raw_tables",
        postgres_conn_id="postgres_raw",
        sql="raw_tables.sql",
    )

    create_marts_tables = PostgresOperator(
        task_id="create_marts_tables",
        postgres_conn_id="postgres_marts",
        sql="mart_tables.sql",
    )

    create_raw_tables >> create_marts_tables