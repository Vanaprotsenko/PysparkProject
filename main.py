from pyspark.sql import SparkSession

sfOptions = {
    "sfUser": "IvanProtsenko",
    "sfPassword": "157010206387586As",
    "sfURL": "BMCEWDK-CO98351.snowflakecomputing.com",
    "sfDatabase": "SNOWFLAKE_SAMPLE_DATA",
    "sfSchema": "TPCH_SF1",
    "sfWarehouse": "COMPUTE_WH",
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
