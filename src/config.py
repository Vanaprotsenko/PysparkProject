import os
from dotenv import load_dotenv

load_dotenv()


class BaseConf:
    @staticmethod
    def get_basic_sfoptions():
        return {
            "sfUser": os.environ.get("SFUSER"),
            "sfPassword": os.environ.get("SFPASSWORD"),
            "sfURL": os.environ.get("SFURL"),
            "sfSchema": os.environ.get("SFSCHEMA"),
            "sfWarehouse": os.environ.get("SFWAREHOUS"),
            "sfDatabase": "SNOWFLAKE_SAMPLE_DATA",
            "sfRole": "ACCOUNTADMIN",
        }
