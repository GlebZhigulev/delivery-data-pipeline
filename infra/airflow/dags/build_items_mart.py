from datetime import datetime

from airflow import DAG
from spark.common.spark_session import spark_submit_task

with DAG(
        dag_id="build_items_mart",
        start_date=datetime(2026, 3, 25),
        schedule=None,
        catchup=False,
        tags=["spark", "marts"],
) as dag:
    build_items_mart = spark_submit_task(
        task_id="build_items_mart",
        application="/opt/airflow/spark/jobs/build_items_mart.py",
        dag=dag,
    )
