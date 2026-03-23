from pyspark.sql import functions as F

from spark.common.spark_session import (
    MARTS_DB,
    RAW_DB,
    get_jdbc_url,
    get_jdbc_properties,
    get_spark_session,
    write_table,
)


def build_items_mart() -> None:
    spark = get_spark_session("build_items_mart")

    query = """
    (
        SELECT
            DATE(o.created_at) AS report_date,
            o.store_id AS store_id,
            oi.item_id AS item_id,
            EXTRACT(YEAR FROM o.created_at)::int AS report_year,
            EXTRACT(MONTH FROM o.created_at)::int AS report_month,
            EXTRACT(DAY FROM o.created_at)::int AS report_day,
            o.delivery_city AS city,
            s.store_name AS store_name,
            i.item_category AS category,
            i.item_title AS item,
            SUM(i.item_price * oi.item_quantity)::numeric(12,2) AS turnover,
            SUM(oi.item_quantity)::int AS ordered_quantity,
            SUM(oi.item_canceled_quantity)::int AS canceled_quantity,
            COUNT(DISTINCT o.order_id)::int AS orders_with_item_count,
            COUNT(DISTINCT CASE WHEN oi.item_canceled_quantity > 0 THEN o.order_id END)::int AS canceled_orders_with_item_count
        FROM public.order_items oi
        JOIN public.orders o
            ON oi.order_id = o.order_id
        JOIN public.items i
            ON oi.item_id = i.item_id
        JOIN public.stores s
            ON o.store_id = s.store_id
        GROUP BY
            DATE(o.created_at),
            o.store_id,
            oi.item_id,
            EXTRACT(YEAR FROM o.created_at),
            EXTRACT(MONTH FROM o.created_at),
            EXTRACT(DAY FROM o.created_at),
            o.delivery_city,
            s.store_name,
            i.item_category,
            i.item_title
    ) AS mart_items_src
    """

    result_df = (
        spark.read.format("jdbc")
        .option("url", get_jdbc_url(RAW_DB))
        .option("dbtable", query)
        .option("user", get_jdbc_properties()["user"])
        .option("password", get_jdbc_properties()["password"])
        .option("driver", get_jdbc_properties()["driver"])
        .load()
    )

    result_df = result_df.coalesce(1)

    write_table(result_df, MARTS_DB, "mart_items")
    spark.stop()


def main() -> None:
    build_items_mart()


if __name__ == "__main__":
    main()