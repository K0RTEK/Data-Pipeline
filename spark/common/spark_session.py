import psycopg2
from pyspark.sql import DataFrame, SparkSession

POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432

RAW_DB = "postgres"
MARTS_DB = "postgres"

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

POSTGRES_DRIVER = "org.postgresql.Driver"


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