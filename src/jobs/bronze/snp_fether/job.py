from pyspark.sql import SparkSession
from src.utils.utils import initialization_spark_session
from src.config import BaseConf

# conf = BaseConf
#
# sfOptions = conf.get_basic_sfoptions()
# spark = initialization_spark_session()
#
#
# df = spark.read.format("snowflake") \
#     .options(**sfOptions) \
#     .option("query", "SELECT * FROM CUSTOMER LIMIT 10") \
#     .load()
#
# df.show()


class FetchCustomerData:
    def __init__(self, spark: SparkSession, conf: BaseConf):
        self._spark = spark
        self._conf = conf

    def run(self) -> None:
        self._spark = initialization_spark_session()
        self._conf = self._conf.get_basic_sfoptions()

        df = self._spark.read.format("snowflake") \
            .options(**self._conf) \
            .option("query", "SELECT * FROM CUSTOMER LIMIT 10") \
            .load()

        df.show()
