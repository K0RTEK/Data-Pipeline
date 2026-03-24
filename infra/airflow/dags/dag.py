from datetime import datetime
import glob

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

RAW_PATH = "/opt/airflow/data/raw/*.parquet"
POSTGRES_CONN_ID = "postgres_default"


def split_store_parts(value):
    if value is None or pd.isna(value):
        return None, None, None

    parts = [p.strip() for p in str(value).split(",", maxsplit=2)]

    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return parts[0], parts[1], None
    return parts[0], None, None


def split_delivery_parts(value):
    if value is None or pd.isna(value):
        return None, None

    parts = [p.strip() for p in str(value).split(",", maxsplit=1)]

    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], None


def to_python_records(df: pd.DataFrame):
    if df.empty:
        return []

    clean_df = df.copy().astype(object)
    clean_df = clean_df.where(pd.notnull(clean_df), None)
    return [tuple(row) for row in clean_df.itertuples(index=False, name=None)]


def execute_batch(cur, sql: str, df: pd.DataFrame):
    records = to_python_records(df)
    if records:
        execute_values(cur, sql, records, page_size=1000)


def build_entities(df: pd.DataFrame):
    users = (
        df[["user_id", "user_phone"]]
        .drop_duplicates(subset=["user_id"])
        .copy()
    )
    users["user_id"] = users["user_id"].astype(int)

    drivers = (
        df[["driver_id", "driver_phone"]]
        .dropna(subset=["driver_id"])
        .drop_duplicates(subset=["driver_id"])
        .copy()
    )
    if not drivers.empty:
        drivers["driver_id"] = drivers["driver_id"].astype(int)

    items = (
        df[["item_id", "item_title", "item_category", "item_price"]]
        .drop_duplicates(subset=["item_id"])
        .copy()
    )
    items["item_id"] = items["item_id"].astype(int)
    items["item_price"] = items["item_price"].fillna(0).astype(int)

    stores = (
        df[["store_id", "store_address"]]
        .drop_duplicates(subset=["store_id"])
        .copy()
    )
    stores["store_id"] = stores["store_id"].astype(int)

    store_parts = stores["store_address"].apply(split_store_parts)
    stores["store_name"] = store_parts.apply(lambda x: x[0])
    stores["store_city"] = store_parts.apply(lambda x: x[1])
    stores["store_address"] = store_parts.apply(lambda x: x[2])

    stores["store_name"] = stores["store_name"].fillna("")
    stores["store_city"] = stores["store_city"].fillna("")
    stores["store_address"] = stores["store_address"].fillna("")

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
        .drop_duplicates(subset=["order_id"])
        .copy()
    )

    orders["order_id"] = orders["order_id"].astype(int)
    orders["user_id"] = orders["user_id"].astype(int)
    orders["store_id"] = orders["store_id"].astype(int)
    orders["order_discount"] = orders["order_discount"].fillna(0).astype(int)
    orders["payment_type"] = orders["payment_type"].fillna("")

    delivery_parts = orders["address_text"].apply(split_delivery_parts)
    orders["delivery_city"] = delivery_parts.apply(lambda x: x[0])
    orders["delivery_address"] = delivery_parts.apply(lambda x: x[1])

    orders["delivery_city"] = orders["delivery_city"].fillna("")
    orders["delivery_address"] = orders["delivery_address"].fillna("")

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
        .dropna(subset=["driver_id"])
        .drop_duplicates(subset=["order_id", "driver_id"])
        .copy()
    )
    if not order_drivers.empty:
        order_drivers["order_id"] = order_drivers["order_id"].astype(int)
        order_drivers["driver_id"] = order_drivers["driver_id"].astype(int)
        order_drivers["delivery_cost"] = order_drivers["delivery_cost"].fillna(0).astype(int)

    order_items = df[
        [
            "order_id",
            "item_id",
            "item_quantity",
            "item_canceled_quantity",
            "item_discount",
            "item_replaced_id",
        ]
    ].copy()

    order_items["order_id"] = order_items["order_id"].astype(int)
    order_items["item_id"] = order_items["item_id"].astype(int)
    order_items["item_quantity"] = order_items["item_quantity"].fillna(0).astype(int)
    order_items["item_canceled_quantity"] = order_items["item_canceled_quantity"].fillna(0).astype(int)
    order_items["item_discount"] = order_items["item_discount"].fillna(0).astype(int)
    order_items["item_replaced_id"] = order_items["item_replaced_id"].astype("Int64")

    order_items = (
        order_items
        .groupby(
            ["order_id", "item_id", "item_discount", "item_replaced_id"],
            dropna=False,
            as_index=False,
        )
        .agg(
            {
                "item_quantity": "sum",
                "item_canceled_quantity": "sum",
            }
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


def upsert_users(cur, users: pd.DataFrame):
    if users.empty:
        return

    cur.execute(
        """
        CREATE TEMP TABLE tmp_users (
            user_id integer,
            user_phone text
        ) ON COMMIT DROP
        """
    )

    execute_batch(
        cur,
        """
        INSERT INTO tmp_users (user_id, user_phone)
        VALUES %s
        """,
        users,
    )

    cur.execute(
        """
        UPDATE users u
        SET user_phone = t.user_phone
        FROM tmp_users t
        WHERE u.user_id = t.user_id
        """
    )

    cur.execute(
        """
        INSERT INTO users (user_id, user_phone)
        SELECT t.user_id, t.user_phone
        FROM tmp_users t
        LEFT JOIN users u ON u.user_id = t.user_id
        WHERE u.user_id IS NULL
        """
    )


def upsert_drivers(cur, drivers: pd.DataFrame):
    if drivers.empty:
        return

    cur.execute(
        """
        CREATE TEMP TABLE tmp_drivers (
            driver_id integer,
            driver_phone text
        ) ON COMMIT DROP
        """
    )

    execute_batch(
        cur,
        """
        INSERT INTO tmp_drivers (driver_id, driver_phone)
        VALUES %s
        """,
        drivers,
    )

    cur.execute(
        """
        UPDATE drivers d
        SET driver_phone = t.driver_phone
        FROM tmp_drivers t
        WHERE d.driver_id = t.driver_id
        """
    )

    cur.execute(
        """
        INSERT INTO drivers (driver_id, driver_phone)
        SELECT t.driver_id, t.driver_phone
        FROM tmp_drivers t
        LEFT JOIN drivers d ON d.driver_id = t.driver_id
        WHERE d.driver_id IS NULL
        """
    )


def upsert_items(cur, items: pd.DataFrame):
    if items.empty:
        return

    cur.execute(
        """
        CREATE TEMP TABLE tmp_items (
            item_id integer,
            item_title text,
            item_category text,
            item_price integer
        ) ON COMMIT DROP
        """
    )

    execute_batch(
        cur,
        """
        INSERT INTO tmp_items (item_id, item_title, item_category, item_price)
        VALUES %s
        """,
        items,
    )

    cur.execute(
        """
        UPDATE items i
        SET item_title = t.item_title,
            item_category = t.item_category,
            item_price = t.item_price
        FROM tmp_items t
        WHERE i.item_id = t.item_id
        """
    )

    cur.execute(
        """
        INSERT INTO items (item_id, item_title, item_category, item_price)
        SELECT t.item_id, t.item_title, t.item_category, t.item_price
        FROM tmp_items t
        LEFT JOIN items i ON i.item_id = t.item_id
        WHERE i.item_id IS NULL
        """
    )


def upsert_stores(cur, stores: pd.DataFrame):
    if stores.empty:
        return

    cur.execute(
        """
        CREATE TEMP TABLE tmp_stores (
            store_id integer,
            store_name text,
            store_city text,
            store_address text
        ) ON COMMIT DROP
        """
    )

    execute_batch(
        cur,
        """
        INSERT INTO tmp_stores (store_id, store_name, store_city, store_address)
        VALUES %s
        """,
        stores,
    )

    cur.execute(
        """
        UPDATE stores s
        SET store_name = t.store_name,
            store_city = t.store_city,
            store_address = t.store_address
        FROM tmp_stores t
        WHERE s.store_id = t.store_id
        """
    )

    cur.execute(
        """
        INSERT INTO stores (store_id, store_name, store_city, store_address)
        SELECT t.store_id, t.store_name, t.store_city, t.store_address
        FROM tmp_stores t
        LEFT JOIN stores s ON s.store_id = t.store_id
        WHERE s.store_id IS NULL
        """
    )


def upsert_orders(cur, orders: pd.DataFrame):
    if orders.empty:
        return

    cur.execute(
        """
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
        ) ON COMMIT DROP
        """
    )

    execute_batch(
        cur,
        """
        INSERT INTO tmp_orders (
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
        VALUES %s
        """,
        orders,
    )

    cur.execute(
        """
        UPDATE orders o
        SET user_id = t.user_id,
            store_id = t.store_id,
            created_at = t.created_at,
            paid_at = t.paid_at,
            delivery_started_at = t.delivery_started_at,
            canceled_at = t.canceled_at,
            payment_type = t.payment_type,
            order_cancellation_reason = t.order_cancellation_reason,
            order_discount = t.order_discount,
            delivery_city = t.delivery_city,
            delivery_address = t.delivery_address
        FROM tmp_orders t
        WHERE o.order_id = t.order_id
        """
    )

    cur.execute(
        """
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
            t.order_id,
            t.user_id,
            t.store_id,
            t.created_at,
            t.paid_at,
            t.delivery_started_at,
            t.canceled_at,
            t.payment_type,
            t.order_cancellation_reason,
            t.order_discount,
            t.delivery_city,
            t.delivery_address
        FROM tmp_orders t
        LEFT JOIN orders o ON o.order_id = t.order_id
        WHERE o.order_id IS NULL
        """
    )


def replace_order_children(cur, order_ids, order_drivers: pd.DataFrame, order_items: pd.DataFrame):
    if order_ids:
        cur.execute(
            "DELETE FROM order_items WHERE order_id = ANY(%s)",
            (order_ids,),
        )
        cur.execute(
            "DELETE FROM order_drivers WHERE order_id = ANY(%s)",
            (order_ids,),
        )

    execute_batch(
        cur,
        """
        INSERT INTO order_drivers (
            order_id,
            driver_id,
            delivered_at,
            delivery_cost
        )
        VALUES %s
        """,
        order_drivers,
    )

    execute_batch(
        cur,
        """
        INSERT INTO order_items (
            order_id,
            item_id,
            item_quantity,
            item_canceled_quantity,
            item_discount,
            item_replaced_id
        )
        VALUES %s
        """,
        order_items,
    )


def load_all_parquet_to_core():
    files = sorted(glob.glob(RAW_PATH))
    if not files:
        raise ValueError("No parquet files found in /opt/airflow/data/raw")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False

    try:
        cur = conn.cursor()

        for file_path in files:
            print(f"Processing file: {file_path}")

            df = pd.read_parquet(file_path)
            entities = build_entities(df)

            users = entities["users"]
            drivers = entities["drivers"]
            items = entities["items"]
            stores = entities["stores"]
            orders = entities["orders"]
            order_drivers = entities["order_drivers"]
            order_items = entities["order_items"]

            upsert_users(cur, users)
            upsert_drivers(cur, drivers)
            upsert_items(cur, items)
            upsert_stores(cur, stores)
            upsert_orders(cur, orders)

            order_ids = orders["order_id"].drop_duplicates().tolist()
            replace_order_children(cur, order_ids, order_drivers, order_items)

            conn.commit()

            print(
                f"Loaded {file_path} | "
                f"users={len(users)}, drivers={len(drivers)}, items={len(items)}, "
                f"stores={len(stores)}, orders={len(orders)}, "
                f"order_drivers={len(order_drivers)}, order_items={len(order_items)}"
            )

        cur.close()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def validate_loaded_data():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("SELECT current_database(), current_user;")
    print("Current database and user:", cur.fetchone())

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
        count = cur.fetchone()[0]
        print(f"{table}: {count}")

    cur.close()
    conn.close()


with DAG(
    dag_id="etl_core_load_dag",
    start_date=datetime(2024, 1, 1),
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
