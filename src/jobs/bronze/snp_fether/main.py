from src.utils.utils import initialization_spark_session
from src.config import BaseConf

conf = BaseConf

sfOptions = conf.get_basic_sfoptions()
spark = initialization_spark_session()


df = spark.read.format("snowflake") \
    .options(**sfOptions) \
    .option("query", "SELECT * FROM CUSTOMER LIMIT 10") \
    .load()

df.show()
