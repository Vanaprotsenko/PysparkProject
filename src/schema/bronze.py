from src.config import BaseConf
from src.utils.utils import resolve_by_db_name


class BronzeTable:
    __table_name__: str = Exception('Not implemented')

    @classmethod
    def get_full_name(cls, conf: BaseConf) -> str:
        return f"`{conf.get_basic_sfoptions().get('sfDatabase', None)}` . `{cls.__table_name__}`"

    @classmethod
    def get_db_name(cls) -> str:
        return resolve_by_db_name('bronze')


class Customer(BronzeTable):
    __table_name__ = 'customer'

    c_key = 'customer_key'
    c_name = 'customer_name'
    c_address = 'customer_address'
    c_nation_key = 'customer_nation_key'
    c_phone = 'customer_phone'
    c_acctbal = 'customer_balance'
    c_mktsegment = 'customer_segment'
    c_comment = 'customer_comment'
