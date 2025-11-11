from abc import ABC, abstractmethod
from typing import Literal
from models import ExchangeSymbol
from database import async_upsert_dataframe
from aiohttp import ClientSession


class BaseClient(ABC):
    @abstractmethod
    def base_url(self):
        raise NotImplementedError

    @abstractmethod
    def exchange_id(self):
        raise NotImplementedError

    @abstractmethod
    def inst_type(self):
        raise NotImplementedError

    async def send_request(
        self, method: Literal["GET", "POST"], endpoint: str, params=None, headers=None
    ) -> dict:
        url = f"{self.base_url}{endpoint}"
        async with ClientSession() as session:
            if method == "GET":
                response = await session.get(url, params=params, headers=headers)
            elif method == "POST":
                response = await session.post(url, json=params, headers=headers)
            response.raise_for_status()
            return await response.json()

    @abstractmethod
    async def get_all_symbols(self):
        raise NotImplementedError

    async def update_all_symbols(self):
        df = await self.get_all_symbols()
        await async_upsert_dataframe(
            df,
            ExchangeSymbol,
            [
                "tick_size",
                "step_size",
                "price_precision",
                "quantity_precision",
                "status",
            ],
        )
