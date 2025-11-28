import aiohttp
from aiohttp import ClientTimeout

session: aiohttp.ClientSession | None = None


async def get_session():
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession(timeout=ClientTimeout(total=15))
    return session


async def http_get(url, **kwargs):
    session = await get_session()
    async with session.get(url, **kwargs) as resp:
        return await resp.json()


async def shutdown():
    global session
    if session:
        await session.close()
