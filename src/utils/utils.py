from pyspark.sql import SparkSession


def initialization_spark_session():
    spark = SparkSession.builder \
        .appName("Snowflake Fetch Example") \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.1") \
        .config("spark.executor.memory", "4g") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()

    return spark


def resolve_by_db_name(db_name: str) -> str:
    return db_name
