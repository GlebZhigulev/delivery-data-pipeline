from pyspark.sql import functions as F

from spark.common.spark_session import (
    RAW_DB,
    MARTS_DB,
    get_spark_session,
    read_table,
    write_table,
)

SERVICE_ERROR_REASONS = [
    "Ошибка приложения",
    "Проблемы с оплатой",
]


def build_orders_mart() -> None:
    spark = get_spark_session("build_orders_mart")

    orders = (
        read_table(spark, RAW_DB, "orders")
        .select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("created_at"),
            F.col("delivery_city").alias("city"),
            F.col("store_id").cast("int").alias("store_id"),
            F.col("user_id").cast("int").alias("user_id"),
            F.coalesce(F.col("order_discount"), F.lit(0)).cast("double").alias("order_discount_pct"),
            F.col("canceled_at"),
            F.col("order_cancellation_reason"),
        )
        .alias("o")
    )

    stores = (
        read_table(spark, RAW_DB, "stores")
        .select(
            F.col("store_id").cast("int").alias("store_id"),
            F.col("store_name"),
        )
        .alias("s")
    )

    items = (
        read_table(spark, RAW_DB, "items")
        .select(
            F.col("item_id").cast("int").alias("item_id"),
            F.col("item_price").cast("double").alias("item_price"),
        )
        .alias("i")
    )

    order_items = (
        read_table(spark, RAW_DB, "order_items")
        .select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("item_id").cast("int").alias("item_id"),
            F.col("item_quantity").cast("int").alias("item_quantity"),
            F.col("item_canceled_quantity").cast("int").alias("item_canceled_quantity"),
            F.coalesce(F.col("item_discount"), F.lit(0)).cast("double").alias("item_discount_pct"),
            F.col("item_replaced_id").cast("int").alias("item_replaced_id"),
        )
        .alias("oi")
    )

    order_drivers = (
        read_table(spark, RAW_DB, "order_drivers")
        .select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("driver_id").cast("int").alias("driver_id"),
            F.coalesce(F.col("delivery_cost"), F.lit(0)).cast("double").alias("delivery_cost"),
            F.col("delivered_at"),
        )
        .alias("od")
    )

    item_lines = (
        order_items
        .join(items, F.col("oi.item_id") == F.col("i.item_id"), "inner")
        .select(
            F.col("oi.order_id").alias("order_id"),
            F.col("oi.item_quantity").alias("item_quantity"),
            F.greatest(
                F.col("oi.item_quantity") - F.col("oi.item_canceled_quantity"),
                F.lit(0)
            ).alias("actual_quantity"),
            (
                F.col("i.item_price") *
                (F.lit(1.0) - F.col("oi.item_discount_pct") / F.lit(100.0))
            ).alias("item_price_after_discount"),
            F.col("oi.item_replaced_id").alias("item_replaced_id"),
        )
        .alias("il")
    )

    items_agg = (
        item_lines
        .groupBy("order_id")
        .agg(
            F.sum(
                F.col("item_price_after_discount") * F.col("item_quantity")
            ).cast("decimal(12,2)").alias("turnover_before_order_discount"),

            F.sum(
                F.col("item_price_after_discount") * F.col("actual_quantity")
            ).cast("decimal(12,2)").alias("revenue_before_order_discount"),

            F.max(
                F.when(F.col("item_replaced_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            ).cast("int").alias("has_replacements"),
        )
        .alias("ia")
    )

    drivers_agg = (
        order_drivers
        .groupBy("order_id")
        .agg(
            F.sum("delivery_cost").cast("decimal(12,2)").alias("expenses"),
            F.array_sort(F.collect_set("driver_id")).alias("driver_ids"),
            F.countDistinct("driver_id").cast("int").alias("driver_count"),
            F.max(
                F.when(F.col("delivered_at").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            ).cast("int").alias("has_delivery"),
        )
        .alias("da")
    )

    result_df = (
        orders
        .join(stores, F.col("o.store_id") == F.col("s.store_id"), "inner")
        .join(items_agg, F.col("o.order_id") == F.col("ia.order_id"), "left")
        .join(drivers_agg, F.col("o.order_id") == F.col("da.order_id"), "left")
        .select(
            F.col("o.order_id").alias("order_id"),
            F.year(F.col("o.created_at")).cast("int").alias("order_year"),
            F.month(F.col("o.created_at")).cast("int").alias("order_month"),
            F.dayofmonth(F.col("o.created_at")).cast("int").alias("order_day"),
            F.to_date(F.col("o.created_at")).alias("order_date"),
            F.col("o.city").alias("city"),
            F.col("s.store_name").alias("store_name"),
            F.col("o.store_id").alias("store_id"),
            F.col("o.user_id").alias("user_id"),

            (
                F.coalesce(
                    F.col("ia.turnover_before_order_discount").cast("double"),
                    F.lit(0.0)
                ) * (F.lit(1.0) - F.col("o.order_discount_pct") / F.lit(100.0))
            ).cast("decimal(12,2)").alias("turnover"),

            (
                F.coalesce(
                    F.col("ia.revenue_before_order_discount").cast("double"),
                    F.lit(0.0)
                ) * (F.lit(1.0) - F.col("o.order_discount_pct") / F.lit(100.0))
            ).cast("decimal(12,2)").alias("revenue"),

            F.coalesce(
                F.col("da.expenses"),
                F.lit(0).cast("decimal(12,2)")
            ).cast("decimal(12,2)").alias("expenses"),

            (
                (
                    F.coalesce(
                        F.col("ia.revenue_before_order_discount").cast("double"),
                        F.lit(0.0)
                    ) * (F.lit(1.0) - F.col("o.order_discount_pct") / F.lit(100.0))
                ) - F.coalesce(
                    F.col("da.expenses").cast("double"),
                    F.lit(0.0)
                )
            ).cast("decimal(12,2)").alias("profit"),

            F.lit(1).cast("int").alias("orders_created"),

            F.when(F.coalesce(F.col("da.has_delivery"), F.lit(0)) == 1, F.lit(1))
             .otherwise(F.lit(0))
             .cast("int")
             .alias("orders_delivered"),

            F.when(F.col("o.canceled_at").isNotNull(), F.lit(1))
             .otherwise(F.lit(0))
             .cast("int")
             .alias("orders_canceled"),

            F.when(
                (F.col("o.canceled_at").isNotNull()) &
                (F.coalesce(F.col("da.has_delivery"), F.lit(0)) == 1),
                F.lit(1)
            ).otherwise(F.lit(0))
             .cast("int")
             .alias("orders_canceled_after_delivery"),

            F.when(
                (F.col("o.canceled_at").isNotNull()) &
                (F.col("o.order_cancellation_reason").isin(SERVICE_ERROR_REASONS)),
                F.lit(1)
            ).otherwise(F.lit(0))
             .cast("int")
             .alias("orders_canceled_service_error"),

            F.when(
                F.coalesce(F.col("da.driver_count"), F.lit(0)) > 1,
                F.lit(1)
            ).otherwise(F.lit(0))
             .cast("int")
             .alias("courier_changed"),

            F.when(F.col("da.driver_ids").isNotNull(), F.col("da.driver_ids"))
             .otherwise(F.array().cast("array<int>"))
             .alias("driver_ids"),
        )
    )

    write_table(result_df, MARTS_DB, "mart_orders")
    spark.stop()


def main() -> None:
    build_orders_mart()


if __name__ == "__main__":
    main()