import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from contextlib import asynccontextmanager

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

DATABASE_URL = f"mysql+aiomysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?charset=utf8mb4"

async_engine = create_async_engine(DATABASE_URL, echo=False, future=True)

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine, autoflush=False, autocommit=False
)

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

    update_dict = {f: text(f"VALUES({f})") for f in update_fields}
    upsert_stmt = insert_stmt.on_duplicate_key_update(**update_dict)

    async with async_engine.begin() as conn:
        await conn.execute(upsert_stmt, insert_values)
