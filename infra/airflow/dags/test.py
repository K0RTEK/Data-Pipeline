from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession


def run_spark_job():
    spark = (
        SparkSession.builder
        .appName("TestSparkDAG")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    df = spark.read.parquet("/opt/airflow/data/raw")
    df.createOrReplaceTempView("orders")

    result = spark.sql("""
        SELECT *
        FROM orders
        LIMIT 10
    """)

    result.show(truncate=False)
    spark.stop()


with DAG(
    dag_id="test_spark_parquet_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job,
    )

    task