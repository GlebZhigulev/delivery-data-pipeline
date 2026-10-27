from datetime import datetime
import glob
import io

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_PATH = "/opt/airflow/data/raw/*.parquet"
POSTGRES_CONN_ID = "postgres_raw"

PARQUET_COLUMNS = [
    "user_id",
    "user_phone",
    "driver_id",
    "driver_phone",
    "item_id",
    "item_title",
    "item_category",
    "item_price",
    "store_id",
    "store_address",
    "order_id",
    "created_at",
    "paid_at",
    "delivery_started_at",
    "canceled_at",
    "payment_type",
    "order_cancellation_reason",
    "order_discount",
    "address_text",
    "delivered_at",
    "delivery_cost",
    "item_quantity",
    "item_canceled_quantity",
    "item_discount",
    "item_replaced_id",
]


def copy_df_to_table(cur, df: pd.DataFrame, table_name: str, columns: list[str]) -> None:
    if df.empty:
        return

    export_df = df.copy()

    for col in export_df.columns:
        if pd.api.types.is_datetime64_any_dtype(export_df[col]):
            export_df[col] = export_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    export_df = export_df.where(pd.notnull(export_df), None)

    buf = io.StringIO()
    export_df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
    buf.seek(0)

    cols_sql = ", ".join(columns)
    sql = f"""
        COPY {table_name} ({cols_sql})
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')
    """
    cur.copy_expert(sql, buf)


def build_entities(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    users = (
        df[["user_id", "user_phone"]]
        .dropna(subset=["user_id"])
        .drop_duplicates(subset=["user_id"], keep="last")
        .copy()
    )
    users["user_id"] = users["user_id"].astype("int64")

    drivers = (
        df[["driver_id", "driver_phone"]]
        .dropna(subset=["driver_id"])
        .drop_duplicates(subset=["driver_id"], keep="last")
        .copy()
    )
    if not drivers.empty:
        drivers["driver_id"] = drivers["driver_id"].astype("int64")

    items = (
        df[["item_id", "item_title", "item_category", "item_price"]]
        .dropna(subset=["item_id"])
        .drop_duplicates(subset=["item_id"], keep="last")
        .copy()
    )
    items["item_id"] = items["item_id"].astype("int64")
    items["item_price"] = items["item_price"].fillna(0).astype("int64")

    stores = (
        df[["store_id", "store_address"]]
        .dropna(subset=["store_id"])
        .drop_duplicates(subset=["store_id"], keep="last")
        .copy()
    )
    stores["store_id"] = stores["store_id"].astype("int64")

    store_split = stores["store_address"].fillna("").astype(str).str.split(",", n=2, expand=True)
    while store_split.shape[1] < 3:
        store_split[store_split.shape[1]] = ""

    stores["store_name"] = store_split[0].fillna("").str.strip()
    stores["store_city"] = store_split[1].fillna("").str.strip()
    stores["store_address"] = store_split[2].fillna("").str.strip()
    stores = stores[["store_id", "store_name", "store_city", "store_address"]]

    orders = (
        df[
            [
                "order_id",
                "user_id",
                "store_id",
                "created_at",
                "paid_at",
                "delivery_started_at",
                "canceled_at",
                "payment_type",
                "order_cancellation_reason",
                "order_discount",
                "address_text",
            ]
        ]
        .dropna(subset=["order_id", "user_id", "store_id"])
        .drop_duplicates(subset=["order_id"], keep="last")
        .copy()
    )

    orders["order_id"] = orders["order_id"].astype("int64")
    orders["user_id"] = orders["user_id"].astype("int64")
    orders["store_id"] = orders["store_id"].astype("int64")
    orders["order_discount"] = orders["order_discount"].fillna(0).astype("int64")
    orders["payment_type"] = orders["payment_type"].fillna("")

    delivery_split = orders["address_text"].fillna("").astype(str).str.split(",", n=1, expand=True)
    while delivery_split.shape[1] < 2:
        delivery_split[delivery_split.shape[1]] = ""

    orders["delivery_city"] = delivery_split[0].fillna("").str.strip()
    orders["delivery_address"] = delivery_split[1].fillna("").str.strip()

    orders = orders[
        [
            "order_id",
            "user_id",
            "store_id",
            "created_at",
            "paid_at",
            "delivery_started_at",
            "canceled_at",
            "payment_type",
            "order_cancellation_reason",
            "order_discount",
            "delivery_city",
            "delivery_address",
        ]
    ]

    order_drivers = (
        df[["order_id", "driver_id", "delivered_at", "delivery_cost"]]
        .dropna(subset=["order_id", "driver_id"])
        .drop_duplicates(subset=["order_id", "driver_id"], keep="last")
        .copy()
    )
    if not order_drivers.empty:
        order_drivers["order_id"] = order_drivers["order_id"].astype("int64")
        order_drivers["driver_id"] = order_drivers["driver_id"].astype("int64")
        order_drivers["delivery_cost"] = order_drivers["delivery_cost"].fillna(0).astype("int64")

    order_items = (
        df[
            [
                "order_id",
                "item_id",
                "item_quantity",
                "item_canceled_quantity",
                "item_discount",
                "item_replaced_id",
            ]
        ]
        .dropna(subset=["order_id", "item_id"])
        .copy()
    )

    if not order_items.empty:
        order_items["order_id"] = order_items["order_id"].astype("int64")
        order_items["item_id"] = order_items["item_id"].astype("int64")
        order_items["item_quantity"] = order_items["item_quantity"].fillna(0).astype("int64")
        order_items["item_canceled_quantity"] = order_items["item_canceled_quantity"].fillna(0).astype("int64")
        order_items["item_discount"] = order_items["item_discount"].fillna(0).astype("int64")
        order_items["item_replaced_id"] = order_items["item_replaced_id"].astype("Int64")

        order_items = (
            order_items.groupby(
                ["order_id", "item_id", "item_discount", "item_replaced_id"],
                dropna=False,
                as_index=False,
            )
            .agg(
                item_quantity=("item_quantity", "sum"),
                item_canceled_quantity=("item_canceled_quantity", "sum"),
            )
        )

        order_items = order_items[
            [
                "order_id",
                "item_id",
                "item_quantity",
                "item_canceled_quantity",
                "item_discount",
                "item_replaced_id",
            ]
        ]

    return {
        "users": users,
        "drivers": drivers,
        "items": items,
        "stores": stores,
        "orders": orders,
        "order_drivers": order_drivers,
        "order_items": order_items,
    }


def create_temp_tables(cur) -> None:
    cur.execute(
        """
        CREATE TEMP TABLE tmp_users (
            user_id integer,
            user_phone text
        ) ON COMMIT DROP;

        CREATE TEMP TABLE tmp_drivers (
            driver_id integer,
            driver_phone text
        ) ON COMMIT DROP;

        CREATE TEMP TABLE tmp_items (
            item_id integer,
            item_title text,
            item_category text,
            item_price integer
        ) ON COMMIT DROP;

        CREATE TEMP TABLE tmp_stores (
            store_id integer,
            store_name text,
            store_city text,
            store_address text
        ) ON COMMIT DROP;

        CREATE TEMP TABLE tmp_orders (
            order_id integer,
            user_id integer,
            store_id integer,
            created_at timestamp,
            paid_at timestamp,
            delivery_started_at timestamp,
            canceled_at timestamp,
            payment_type text,
            order_cancellation_reason text,
            order_discount integer,
            delivery_city text,
            delivery_address text
        ) ON COMMIT DROP;

        CREATE TEMP TABLE tmp_order_drivers (
            order_id integer,
            driver_id integer,
            delivered_at timestamp,
            delivery_cost integer
        ) ON COMMIT DROP;

        CREATE TEMP TABLE tmp_order_items (
            order_id integer,
            item_id integer,
            item_quantity integer,
            item_canceled_quantity integer,
            item_discount integer,
            item_replaced_id integer
        ) ON COMMIT DROP;
        """
    )


def merge_dimensions_and_orders(cur) -> None:
    cur.execute(
        """
        INSERT INTO users (user_id, user_phone)
        SELECT user_id, user_phone
        FROM tmp_users
        ON CONFLICT (user_id) DO UPDATE
        SET user_phone = EXCLUDED.user_phone;

        INSERT INTO drivers (driver_id, driver_phone)
        SELECT driver_id, driver_phone
        FROM tmp_drivers
        ON CONFLICT (driver_id) DO UPDATE
        SET driver_phone = EXCLUDED.driver_phone;

        INSERT INTO items (item_id, item_title, item_category, item_price)
        SELECT item_id, item_title, item_category, item_price
        FROM tmp_items
        ON CONFLICT (item_id) DO UPDATE
        SET item_title = EXCLUDED.item_title,
            item_category = EXCLUDED.item_category,
            item_price = EXCLUDED.item_price;

        INSERT INTO stores (store_id, store_name, store_city, store_address)
        SELECT store_id, store_name, store_city, store_address
        FROM tmp_stores
        ON CONFLICT (store_id) DO UPDATE
        SET store_name = EXCLUDED.store_name,
            store_city = EXCLUDED.store_city,
            store_address = EXCLUDED.store_address;

        INSERT INTO orders (
            order_id,
            user_id,
            store_id,
            created_at,
            paid_at,
            delivery_started_at,
            canceled_at,
            payment_type,
            order_cancellation_reason,
            order_discount,
            delivery_city,
            delivery_address
        )
        SELECT
            order_id,
            user_id,
            store_id,
            created_at,
            paid_at,
            delivery_started_at,
            canceled_at,
            payment_type,
            order_cancellation_reason,
            order_discount,
            delivery_city,
            delivery_address
        FROM tmp_orders
        ON CONFLICT (order_id) DO UPDATE
        SET user_id = EXCLUDED.user_id,
            store_id = EXCLUDED.store_id,
            created_at = EXCLUDED.created_at,
            paid_at = EXCLUDED.paid_at,
            delivery_started_at = EXCLUDED.delivery_started_at,
            canceled_at = EXCLUDED.canceled_at,
            payment_type = EXCLUDED.payment_type,
            order_cancellation_reason = EXCLUDED.order_cancellation_reason,
            order_discount = EXCLUDED.order_discount,
            delivery_city = EXCLUDED.delivery_city,
            delivery_address = EXCLUDED.delivery_address;
        """
    )


def replace_children(cur) -> None:
    cur.execute(
        """
        DELETE FROM order_items oi
        USING (SELECT DISTINCT order_id FROM tmp_orders) t
        WHERE oi.order_id = t.order_id;

        DELETE FROM order_drivers od
        USING (SELECT DISTINCT order_id FROM tmp_orders) t
        WHERE od.order_id = t.order_id;

        INSERT INTO order_drivers (
            order_id,
            driver_id,
            delivered_at,
            delivery_cost
        )
        SELECT
            order_id,
            driver_id,
            delivered_at,
            delivery_cost
        FROM tmp_order_drivers;

        INSERT INTO order_items (
            order_id,
            item_id,
            item_quantity,
            item_canceled_quantity,
            item_discount,
            item_replaced_id
        )
        SELECT
            order_id,
            item_id,
            item_quantity,
            item_canceled_quantity,
            item_discount,
            item_replaced_id
        FROM tmp_order_items;
        """
    )


def load_all_parquet_to_core():
    files = sorted(glob.glob(RAW_PATH))
    if not files:
        raise ValueError(f"No parquet files found by mask: {RAW_PATH}")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for file_path in files:
                df = pd.read_parquet(file_path, columns=PARQUET_COLUMNS)
                entities = build_entities(df)

                create_temp_tables(cur)

                copy_df_to_table(cur, entities["users"], "tmp_users", ["user_id", "user_phone"])
                copy_df_to_table(cur, entities["drivers"], "tmp_drivers", ["driver_id", "driver_phone"])
                copy_df_to_table(cur, entities["items"], "tmp_items", ["item_id", "item_title", "item_category", "item_price"])
                copy_df_to_table(cur, entities["stores"], "tmp_stores", ["store_id", "store_name", "store_city", "store_address"])
                copy_df_to_table(
                    cur,
                    entities["orders"],
                    "tmp_orders",
                    [
                        "order_id",
                        "user_id",
                        "store_id",
                        "created_at",
                        "paid_at",
                        "delivery_started_at",
                        "canceled_at",
                        "payment_type",
                        "order_cancellation_reason",
                        "order_discount",
                        "delivery_city",
                        "delivery_address",
                    ],
                )
                copy_df_to_table(
                    cur,
                    entities["order_drivers"],
                    "tmp_order_drivers",
                    ["order_id", "driver_id", "delivered_at", "delivery_cost"],
                )
                copy_df_to_table(
                    cur,
                    entities["order_items"],
                    "tmp_order_items",
                    [
                        "order_id",
                        "item_id",
                        "item_quantity",
                        "item_canceled_quantity",
                        "item_discount",
                        "item_replaced_id",
                    ],
                )

                merge_dimensions_and_orders(cur)
                replace_children(cur)
                conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def validate_loaded_data():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()

    try:
        with conn.cursor() as cur:
            tables = [
                "users",
                "drivers",
                "items",
                "stores",
                "orders",
                "order_drivers",
                "order_items",
            ]

            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                print(f"{table}: {cur.fetchone()[0]}")
    finally:
        conn.close()


with DAG(
    dag_id="process_raw_data",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "core"],
) as dag:
    load_task = PythonOperator(
        task_id="load_all_parquet_to_core",
        python_callable=load_all_parquet_to_core,
    )

    validate_task = PythonOperator(
        task_id="validate_loaded_data",
        python_callable=validate_loaded_data,
    )

    load_task >> validate_task
