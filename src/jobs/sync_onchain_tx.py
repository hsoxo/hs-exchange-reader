from macro_markets.oklink.fetcher import OklinkOnchainInfo

from databases.doris import get_stream_loader


async def sync_large_transfer():
    stream_loader = get_stream_loader()
    oklink_onchain_info = OklinkOnchainInfo()

    result = await oklink_onchain_info.large_tranfer_monitor()
    await stream_loader.send_rows(result, "onchain_large_transfer")


if __name__ == "__main__":
    import asyncio

    asyncio.run(sync_large_transfer())
