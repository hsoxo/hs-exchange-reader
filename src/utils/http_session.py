import aiohttp
from aiohttp import ClientTimeout

session: aiohttp.ClientSession | None = None

DEFAULT_API_HEADERS = {
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
    "Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
}


async def get_session():
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession(
            timeout=ClientTimeout(total=15),
            headers=DEFAULT_API_HEADERS,
            raise_for_status=False,
        )
    return session


async def http_get(url, **kwargs):
    session = await get_session()
    async with session.get(url, **kwargs) as resp:
        return await resp.json()


async def shutdown():
    global session
    if session:
        await session.close()
