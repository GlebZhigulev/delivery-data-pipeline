import os

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from airflow.operators.bash import BashOperator

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

RAW_DB = os.getenv("RAW_DB", "raw")
MARTS_DB = os.getenv("MARTS_DB", "marts")

POSTGRES_USER = os.getenv("DWH_DB_USER", "admin")
POSTGRES_PASSWORD = os.environ.get("DWH_DB_PASSWORD")

POSTGRES_DRIVER = "org.postgresql.Driver"


def spark_submit_task(
        task_id: str,
        application: str,
        dag,
        jars: str = "/opt/spark/jars/postgresql.jar",
        master: str = "local[*]",
):
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
        cd /opt/airflow && \
        /opt/spark/bin/spark-submit \
          --master {master} \
          --jars {jars} \
          {application}
        """,
        env={
            "JAVA_HOME": "/usr/lib/jvm/java-17",
            "SPARK_HOME": "/opt/spark",
            "PYSPARK_PYTHON": "python3",
            "SPARK_LOCAL_IP": "127.0.0.1",
            "PYTHONPATH": "/opt/airflow",
        },
        append_env=True,  # Preserve DB connection settings from Docker Compose.
        dag=dag,
    )


def get_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_jdbc_url(database: str) -> str:
    return f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{database}"


def get_jdbc_properties() -> dict:
    return {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": POSTGRES_DRIVER,
    }


def read_table(spark: SparkSession, database: str, table_name: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", get_jdbc_url(database))
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .option("fetchsize", "1000")
        .load()
    )


def truncate_table(database: str, table_name: str) -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=database,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name};")
    finally:
        conn.close()


def write_table(df: DataFrame, database: str, table_name: str) -> None:
    truncate_table(database, table_name)

    (
        df.write.format("jdbc")
        .option("url", get_jdbc_url(database))
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .mode("append")
        .save()
    )
