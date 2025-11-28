from contextlib import asynccontextmanager
import os

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from .models import ExchangeInfo, ExchangeSymbol

__all__ = ["ExchangeInfo", "ExchangeSymbol", "async_engine", "async_upsert_dataframe", "get_session", "sync_engine"]

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")

DATABASE_URL = f"mysql+aiomysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
async_engine = create_async_engine(DATABASE_URL, echo=False, future=True)

SYNC_DATABASE_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
)
sync_engine = create_engine(SYNC_DATABASE_URL, echo=False, future=True)

AsyncSessionLocal = async_sessionmaker(bind=async_engine, autoflush=False, autocommit=False)

Base = declarative_base()


@asynccontextmanager
async def get_session():
    async with AsyncSessionLocal() as session:
        yield session


async def async_upsert_dataframe(df: pd.DataFrame, model, update_fields: list[str]):
    """
    异步批量 upsert DataFrame 到 MySQL 表中（支持 ON DUPLICATE KEY UPDATE）
    """
    if df.empty:
        print("⚠️ DataFrame is empty, skip upsert.")
        return

    table = model.__table__
    valid_cols = [c.name for c in table.columns]
    df = df[[c for c in df.columns if c in valid_cols]]

    insert_stmt = insert(table)
    insert_values = df.to_dict(orient="records")

    update_dict = {f: insert_stmt.inserted[f] for f in update_fields}
    upsert_stmt = insert_stmt.on_duplicate_key_update(**update_dict)

    async with async_engine.begin() as conn:
        await conn.execute(upsert_stmt, insert_values)


async def async_upsert(values: list[dict], model, update_fields: list[str]):
    """
    异步批量 upsert 到 MySQL 表中（支持 ON DUPLICATE KEY UPDATE）
    """
    if not values:
        print("⚠️ Empty values, skip upsert.")
        return

    table = model.__table__
    valid_cols = [c.name for c in table.columns]
    insert_values = [{k: v for k, v in row.items() if k in valid_cols} for row in values]

    insert_stmt = insert(table)

    update_dict = {f: insert_stmt.inserted[f] for f in update_fields}
    upsert_stmt = insert_stmt.on_duplicate_key_update(**update_dict)

    async with async_engine.begin() as conn:
        await conn.execute(upsert_stmt, insert_values)
