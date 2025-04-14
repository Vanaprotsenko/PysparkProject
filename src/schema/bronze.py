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
