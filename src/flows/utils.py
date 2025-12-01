from constants import InstType
from sqlalchemy import select
from sqlalchemy.orm import Session

from databases.mysql import sync_engine
from databases.mysql.models import ExchangeInfo, ExchangeSymbol


async def get_symbols(exchange: str, base_asset: [str], quote_asset: str, inst_type: InstType):
    with Session(sync_engine) as conn:
        results = (
            select(ExchangeSymbol)
            .join(ExchangeInfo)
            .where(ExchangeSymbol.base_asset.in_(base_asset))
            .where(ExchangeSymbol.quote_asset == quote_asset)
            .where(ExchangeInfo.name == exchange)
            .where(ExchangeSymbol.inst_type == inst_type.value)
        )
        symbols = conn.execute(results).scalars().all()
    return symbols


async def get_exchange_info(exchange: str):
    with Session(sync_engine) as conn:
        results = select(ExchangeInfo).where(ExchangeInfo.name == exchange)
        exchange_info = conn.execute(results).scalar_one_or_none()
    return exchange_info
