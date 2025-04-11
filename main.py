import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

sfOptions = {
    "sfUser": os.environ.get("SFUSER"),
    "sfPassword": os.environ.get("SFPASSWORD"),
    "sfURL": os.environ.get("SFURL"),
    "sfSchema": os.environ.get("SFSCHEMA"),
    "sfWarehouse": os.environ.get("SFWAREHOUS"),
    "sfDatabase": "SNOWFLAKE_SAMPLE_DATA",
    "sfRole": "ACCOUNTADMIN",
}

spark = SparkSession.builder \
    .appName("Snowflake Fetch Example") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.1") \
    .config("spark.executor.memory", "4g") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()


df = spark.read.format("snowflake") \
    .options(**sfOptions) \
    .option("query", "SELECT * FROM CUSTOMER LIMIT 10") \
    .load()

df.show()
