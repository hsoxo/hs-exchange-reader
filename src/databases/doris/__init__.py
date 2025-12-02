from functools import lru_cache
from io import BytesIO
import json
import os
from urllib.parse import quote

import aiohttp
from dotenv import load_dotenv
from prefect import get_run_logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()


class DorisAsyncDB:
    def __init__(self):
        self.logger = get_run_logger()
        db_host = os.getenv("DORIS_HOST", "127.0.0.1")
        db_user = os.getenv("DORIS_USER", "root")
        db_pass = os.getenv("DORIS_PASSWORD", "")
        db_name = os.getenv("DORIS_DB", "default")

        self.DATABASE_URL = f"mysql+aiomysql://{db_user}:{db_pass}@{db_host}:9030/{db_name}"
        self.engine = create_async_engine(
            self.DATABASE_URL,
            echo=False,
            pool_pre_ping=True,
        )

        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    async def get_session(self) -> AsyncSession:
        async with self.SessionLocal() as session:
            yield session

    async def query(self, sql: str, params: dict | None = None):
        """
        执行查询语句，返回与 ClickHouse client 相同风格的结构：
        result.result_rows = [(...), (...)]
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(sql), params or {})
            rows = result.fetchall()
            return rows

    async def execute(self, sql: str, params: dict | None = None):
        """
        执行写入类语句 (INSERT/UPDATE/DELETE)
        """
        async with self.engine.begin() as conn:
            await conn.execute(text(sql), params or {})


class DorisStreamLoader:
    def __init__(self):
        self.logger = get_run_logger()
        self.host = os.environ.get("DORIS_HOST")
        self.http_port = os.environ.get("DORIS_HTTP_PORT", "8030")  # FE HTTP PORT
        self.user = os.environ.get("DORIS_USER")
        self.password = quote(os.environ.get("DORIS_PASSWORD", ""))
        self.database = os.getenv("DORIS_DB", "default")

        if not self.host or not self.user:
            raise Exception("DORIS_HOST and DORIS_USER must be set")

    # -----------------------------
    # Internal: low-level streamload
    # -----------------------------
    async def _send_streamload_request_async(
        self,
        url: str,
        data,
        headers: dict,
        auth: tuple[str, str],
    ):
        """
        异步 StreamLoad（aiohttp 版）
        支持 Doris FE → BE 的 307/308 PUT 重定向，并保持认证信息。
        """

        username, password = auth

        async with aiohttp.ClientSession() as session:
            # aiohttp 的 BasicAuth
            aio_auth = aiohttp.BasicAuth(username, password)

            # ⚠ aiohttp PUT 必须把 BytesIO 转成 raw content
            payload = data.getvalue() if hasattr(data, "getvalue") else data

            async with session.put(
                url,
                data=payload,
                headers=headers,
                auth=aio_auth,
            ) as resp:
                try:
                    text = await resp.text()
                    result = json.loads(text)
                except Exception as e:
                    raise Exception(f"StreamLoad response not JSON: {resp.text}") from e
                return resp, result

    # -----------------------------
    # Public: DataFrame → Doris
    # -----------------------------
    # def send_dataframe(self, df: pd.DataFrame, database: str, table: str, **kwargs):
    #     """
    #     将 DataFrame 通过 StreamLoad 写入 Doris。
    #     """
    #     # DataFrame => CSV (no header)
    #     csv_data = df.to_csv(index=False, header=False, encoding="utf-8")

    #     # Basic StreamLoad headers
    #     headers = {
    #         "Expect": "100-continue",
    #         "column_separator": "\t",
    #         "enclose": '"',
    #         "trim_double_quotes": "true",
    #         "line_delimiter": r"\n",
    #         "columns": ",".join(df.columns),
    #     }

    #     # Allow extended headers
    #     headers.update(kwargs)

    #     # Doris StreamLoad URL
    #     streamload_url = f"http://{self.host}:{self.http_port}/api/{database}/{table}/_stream_load"

    #     # Perform request
    #     resp = self._send_streamload_request(
    #         streamload_url,
    #         data=BytesIO(csv_data.encode("utf-8")),
    #         headers=headers,
    #         auth=(self.user, self.password),
    #     )

    #     # Check result
    #     try:
    #         result = resp.json()
    #     except Exception:
    #         raise Exception(f"StreamLoad response not JSON: {resp.text}")

    #     if resp.status_code == 200 and result.get("Status") == "Success":
    #         return result
    #     else:
    #         raise Exception(f"StreamLoad to {database}.{table} failed: {result}")

    async def send_rows(self, rows, table: str, column_names: list[str] | None = None, **kwargs):
        """
        写入 Doris StreamLoad:
        - rows: list[dict] or list[list]
        - column_names: required for list[list]，可选 for list[dict]
        """
        if not rows:
            return
        # -------------------
        # 1. 处理 list[dict]
        # -------------------
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            # 自动抽字段
            if column_names is None:
                column_names = list(rows[0].keys())

            csv_lines = []
            for row in rows:
                line = "\t".join("" if row.get(col) is None else str(row.get(col)) for col in column_names)
                csv_lines.append(line)

            csv_data = "\n".join(csv_lines)

        # -------------------
        # 2. 处理 list[list]
        # -------------------
        elif isinstance(rows, list) and rows and isinstance(rows[0], list):
            if column_names is None:
                raise ValueError("column_names is required when rows is list[list]")

            csv_lines = []
            for row in rows:
                line = "\t".join("" if v is None else str(v) for v in row)
                csv_lines.append(line)

            csv_data = "\n".join(csv_lines)

        # -------------------
        # 3. 兜底：兼容 pandas DataFrame
        # -------------------
        elif hasattr(rows, "to_csv"):  # pandas DataFrame
            column_names = list(rows.columns)
            csv_data = rows.to_csv(index=False, header=False, encoding="utf-8")

        else:
            raise ValueError("rows must be list[dict], list[list], or DataFrame")

        # -------------------
        # StreamLoad headers
        # -------------------
        headers = {
            "Expect": "100-continue",
            "column_separator": r"\t",
            "enclose": '"',
            "trim_double_quotes": "true",
            "line_delimiter": r"\n",
            "columns": ",".join(column_names),
        }
        headers.update(kwargs)

        streamload_url = f"http://{self.host}:{self.http_port}/api/{self.database}/{table}/_stream_load"

        resp, result = await self._send_streamload_request_async(
            streamload_url,
            data=BytesIO(csv_data.encode("utf-8")),
            headers=headers,
            auth=(self.user, self.password),
        )

        if resp.status == 200 and result.get("Status") == "Success":
            return result
        else:
            self.logger.info(column_names)
            self.logger.info(csv_data)
            self.logger.error(f"StreamLoad to {self.database}.{table} failed: {result}")
            raise Exception(f"StreamLoad to {self.database}.{table} failed: {result}")


# ------------------
# Singleton Instance
# ------------------
@lru_cache
def get_doris() -> DorisAsyncDB:
    return DorisAsyncDB()


@lru_cache
def get_stream_loader() -> DorisStreamLoader:
    return DorisStreamLoader()
