from spark.common.spark_session import (
    RAW_DB,
    MARTS_DB,
    get_jdbc_properties,
    get_jdbc_url,
    get_spark_session,
    write_table,
)

SERVICE_ERROR_REASONS = [
    "technical_error",
    "service_error",
    "app_error",
]


def build_orders_mart() -> None:
    spark = get_spark_session("build_orders_mart")

    service_error_sql = ", ".join(f"'{x}'" for x in SERVICE_ERROR_REASONS)

    query = f"""
    (
        WITH items_agg AS (
            SELECT
                oi.order_id,
                SUM(i.item_price * oi.item_quantity)::numeric(12,2) AS turnover_before_discount,
                SUM(
                    (i.item_price - COALESCE(oi.item_discount, 0))
                    * (oi.item_quantity - oi.item_canceled_quantity)
                )::numeric(12,2) AS revenue_before_order_discount
            FROM public.order_items oi
            JOIN public.items i
                ON oi.item_id = i.item_id
            GROUP BY oi.order_id
        ),
        drivers_agg AS (
            SELECT
                od.order_id,
                SUM(od.delivery_cost)::numeric(12,2) AS expenses,
                ARRAY_AGG(DISTINCT od.driver_id ORDER BY od.driver_id) AS driver_ids,
                COUNT(DISTINCT od.driver_id)::int AS driver_count,
                MAX(CASE WHEN od.delivered_at IS NOT NULL THEN 1 ELSE 0 END)::int AS has_delivery
            FROM public.order_drivers od
            GROUP BY od.order_id
        )
        SELECT
            o.order_id,
            EXTRACT(YEAR FROM o.created_at)::int AS order_year,
            EXTRACT(MONTH FROM o.created_at)::int AS order_month,
            EXTRACT(DAY FROM o.created_at)::int AS order_day,
            DATE(o.created_at) AS order_date,
            o.delivery_city AS city,
            s.store_name AS store_name,
            o.store_id,
            o.user_id,
            COALESCE(ia.turnover_before_discount, 0)::numeric(12,2) AS turnover,
            GREATEST(
                COALESCE(ia.revenue_before_order_discount, 0) - COALESCE(o.order_discount, 0),
                0
            )::numeric(12,2) AS revenue,
            COALESCE(da.expenses, 0)::numeric(12,2) AS expenses,
            (
                GREATEST(
                    COALESCE(ia.revenue_before_order_discount, 0) - COALESCE(o.order_discount, 0),
                    0
                ) - COALESCE(da.expenses, 0)
            )::numeric(12,2) AS profit,
            1::int AS orders_created,
            CASE
                WHEN COALESCE(da.has_delivery, 0) = 1 THEN 1
                ELSE 0
            END::int AS orders_delivered,
            CASE
                WHEN o.canceled_at IS NOT NULL THEN 1
                ELSE 0
            END::int AS orders_canceled,
            CASE
                WHEN o.canceled_at IS NOT NULL AND COALESCE(da.has_delivery, 0) = 1 THEN 1
                ELSE 0
            END::int AS orders_canceled_after_delivery,
            CASE
                WHEN o.canceled_at IS NOT NULL
                     AND o.order_cancellation_reason IN ({service_error_sql})
                THEN 1
                ELSE 0
            END::int AS orders_canceled_service_error,
            CASE
                WHEN COALESCE(da.driver_count, 0) > 1 THEN 1
                ELSE 0
            END::int AS courier_changed,
            COALESCE(da.driver_ids, ARRAY[]::integer[]) AS driver_ids
        FROM public.orders o
        JOIN public.stores s
            ON o.store_id = s.store_id
        LEFT JOIN items_agg ia
            ON o.order_id = ia.order_id
        LEFT JOIN drivers_agg da
            ON o.order_id = da.order_id
    ) AS mart_orders_src
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

    write_table(result_df, MARTS_DB, "mart_orders")
    spark.stop()


def main() -> None:
    build_orders_mart()


if __name__ == "__main__":
    main()