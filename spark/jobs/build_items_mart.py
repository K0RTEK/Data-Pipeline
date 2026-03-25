from pyspark.sql import functions as F

from spark.common.spark_session import (
    RAW_DB,
    MARTS_DB,
    get_spark_session,
    read_table,
    write_table,
)


def build_items_mart() -> None:
    spark = get_spark_session("build_items_mart")

    orders = (
        read_table(spark, RAW_DB, "orders")
        .select(
            "order_id",
            "store_id",
            "created_at",
            "delivery_city",
        )
        .alias("o")
    )

    order_items = (
        read_table(spark, RAW_DB, "order_items")
        .select(
            "order_id",
            "item_id",
            "item_quantity",
            "item_canceled_quantity",
        )
        .alias("oi")
    )

    items = (
        read_table(spark, RAW_DB, "items")
        .select(
            "item_id",
            "item_title",
            "item_category",
            "item_price",
        )
        .alias("i")
    )

    stores = (
        read_table(spark, RAW_DB, "stores")
        .select(
            "store_id",
            "store_name",
        )
        .alias("s")
    )

    source_df = (
        order_items
        .join(orders, F.col("oi.order_id") == F.col("o.order_id"), "inner")
        .join(items, F.col("oi.item_id") == F.col("i.item_id"), "inner")
        .join(stores, F.col("o.store_id") == F.col("s.store_id"), "inner")
        .select(
            F.to_date(F.col("o.created_at")).alias("report_date"),
            F.col("o.store_id").cast("int").alias("store_id"),
            F.col("oi.item_id").cast("int").alias("item_id"),
            F.col("o.delivery_city").alias("city"),
            F.col("s.store_name").alias("store_name"),
            F.col("i.item_category").alias("category"),
            F.col("i.item_title").alias("item"),
            F.col("o.order_id").cast("int").alias("order_id"),
            F.col("i.item_price").cast("decimal(12,2)").alias("item_price"),
            F.col("oi.item_quantity").cast("int").alias("item_quantity"),
            F.col("oi.item_canceled_quantity").cast("int").alias("item_canceled_quantity"),
        )
        .withColumn("report_year", F.year("report_date"))
        .withColumn("report_month", F.month("report_date"))
        .withColumn("report_day", F.dayofmonth("report_date"))
    )
    
    source_df = source_df.repartition(
        1,
        "report_date",
        "store_id",
        "item_id",
    )

    result_df = (
        source_df
        .groupBy(
            "report_date",
            "store_id",
            "item_id",
            "report_year",
            "report_month",
            "report_day",
            "city",
            "store_name",
            "category",
            "item",
        )
        .agg(
            F.sum(F.col("item_price") * F.col("item_quantity"))
                .cast("decimal(12,2)")
                .alias("turnover"),
            F.sum("item_quantity")
                .cast("int")
                .alias("ordered_quantity"),
            F.sum("item_canceled_quantity")
                .cast("int")
                .alias("canceled_quantity"),
            F.countDistinct("order_id")
                .cast("int")
                .alias("orders_with_item_count"),
            F.countDistinct(
                F.when(F.col("item_canceled_quantity") > 0, F.col("order_id"))
            ).cast("int").alias("canceled_orders_with_item_count"),
        )
        .select(
            "report_date",
            "store_id",
            "item_id",
            "report_year",
            "report_month",
            "report_day",
            "city",
            "store_name",
            "category",
            "item",
            "turnover",
            "ordered_quantity",
            "canceled_quantity",
            "orders_with_item_count",
            "canceled_orders_with_item_count",
        )
    )

    result_df = result_df.coalesce(1)

    write_table(result_df, MARTS_DB, "mart_items")
    spark.stop()


def main() -> None:
    build_items_mart()


if __name__ == "__main__":
    main()