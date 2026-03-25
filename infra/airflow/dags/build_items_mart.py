from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="build_items_mart",
    start_date=datetime(2026, 3, 25),
    schedule=None,
    catchup=False,
    tags=["spark", "marts"],
) as dag:

    build_items_mart = SparkSubmitOperator(
        task_id="build_items_mart",
        application="/opt/airflow/spark/jobs/build_items_mart.py",
        conn_id="spark_default",
        name="build_items_mart",
        jars="/opt/spark/jars/postgresql.jar",
        verbose=False,
    )