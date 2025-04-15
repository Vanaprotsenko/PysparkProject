from src.jobs.bronze.snp_fether.job import FetchCustomerData
from src.utils.utils import initialization_spark_session
from src.config import BaseConf


def execute() -> None:
    conf = BaseConf
    with initialization_spark_session() as spark:
        FetchCustomerData(spark, conf).run()


if __name__ == "__main__":
    execute()
