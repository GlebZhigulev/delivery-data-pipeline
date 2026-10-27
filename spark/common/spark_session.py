import os

import psycopg2
from psycopg2 import sql
from uuid import uuid4
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


def write_table(df: DataFrame, database: str, table_name: str) -> None:
    """Atomically publish a full-refresh mart using an isolated staging table.

    Spark writes to the staging table first. The published table is replaced
    inside a single PostgreSQL transaction, so a failed Spark job cannot
    leave the existing mart empty or partially refreshed.
    """
    allowed_targets = {"mart_orders", "mart_items"}
    if database != MARTS_DB or table_name not in allowed_targets:
        raise ValueError(f"Unexpected mart target: {database}.{table_name}")

    staging_table = f"_stg_{table_name}_{uuid4().hex[:12]}"
    target_identifier = sql.Identifier("public", table_name)
    staging_identifier = sql.Identifier("public", staging_table)

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=database,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    conn.autocommit = False
    staging_created = False

    try:
        # Staging table must be committed before Spark JDBC (other DB sessions)
        # can see and populate it. "INCLUDING ALL" preserves the column types,
        # defaults and constraints of the destination table.
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(
                    staging_identifier, target_identifier
                )
            )
        conn.commit()
        staging_created = True

        # If this step fails, the published mart has not been touched.
        (
            df.write.format("jdbc")
            .option("url", get_jdbc_url(database))
            .option("dbtable", f"public.{staging_table}")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", POSTGRES_DRIVER)
            .mode("append")
            .save()
        )

        # TRUNCATE and INSERT are in the SAME transaction. If INSERT fails,
        # PostgreSQL rolls back TRUNCATE and keeps the old published data.
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(staging_identifier)
            )
            row_count = cur.fetchone()[0]
            if row_count == 0:
                raise ValueError(
                    f"Staging table for {table_name} is empty; "
                    "refusing to clear the published mart"
                )

            cur.execute(sql.SQL("TRUNCATE TABLE {}").format(target_identifier))
            cur.execute(
                sql.SQL("INSERT INTO {} SELECT * FROM {}").format(
                    target_identifier, staging_identifier
                )
            )
            cur.execute(sql.SQL("DROP TABLE {}").format(staging_identifier))

        conn.commit()
        staging_created = False
        print(f"Published {row_count} rows to {database}.{table_name}")

    except Exception:
        conn.rollback()
        if staging_created:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("DROP TABLE IF EXISTS {}").format(
                            staging_identifier
                        )
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                # Do not hide the original failure. If cleanup also fails,
                # the uniquely named stage can be removed later.
        raise
    finally:
        conn.close()
