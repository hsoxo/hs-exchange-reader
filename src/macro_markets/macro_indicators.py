import asyncio
from concurrent.futures import ThreadPoolExecutor

import yfinance as yf

from databases.doris import get_stream_loader

MACRO_SYMBOLS = {
    # Equity Index Futures 美股指数期货 - 全球风险偏好核心指标
    "SPX_FUT": "ES=F",  # 标普500指数期货 - 全球风险资产锚定物
    "NDX_FUT": "NQ=F",  # 纳斯达克100期货 - 科技股/成长股的主要风向标
    "DOW_FUT": "YM=F",  # 道琼斯工业指数期货 - 传统行业强弱指标
    # Volatility 波动率 市场恐慌指数
    "VIX_FUT": "VXF",  # VIX波动率期货 - 恐慌/避险程度的核心指标
    # Rates (Treasury Futures)  国债收益率期货 - 反映利率预期
    "US2Y": "ZT=F",  # 美国2年期国债期货 - 利率预期最敏感
    "US10Y": "ZN=F",  # 美国10年期国债期货 - 决定科技股估值的重要因子
    # Commodities 大宗商品 - 反映通胀与经济周期
    "OIL_WTI": "CL=F",  # 原油 WTI - 通胀预期与经济强弱的重要指示器
    "GOLD": "GC=F",  # 黄金 - 避险资产
    # FX 外汇 - 大类资产的流动性与风险偏好核心
    "DXY": "DX-Y.NYB",  # 美元指数 - 全球流动性与风险偏好总开关
    "USDJPY": "JPY=X",  # 美元兑日元 - carry trade核心 risk-on/off最灵敏风向
    "USDCNH": "CNH=X",  # 美元兑离岸人民币 - 亚洲风险偏好 资金流向
    # Europe / Asia 全球其他重要股市
    "FTSE": "^FTSE",  # 英国FTSE指数 - 能源与金融权重高
    "NIKKEI": "^N225",  # 日本日经225指数 - 亚洲资金风向
    "HSI": "^HSI",  # 香港恒生指数 - 亚洲风险偏好 对加密市场相关性高
    # A-share China A股
    "CSI300": "000300.SS",  # 沪深300指数 - A股风向标
    "SSEC": "000001.SS",  # 上证指数 - A股整体代表
}


def _download_symbol(yf_symbol: str):
    """使用同步的 yfinance 下载单个 symbol 的数据"""
    return yf.download(
        tickers=yf_symbol,
        interval="1m",
        period="1d",
        progress=False,
        auto_adjust=False,
    )


executor = ThreadPoolExecutor(max_workers=8)


async def get_macro_klines(logger):
    loop = asyncio.get_running_loop()
    results = []

    tasks = {
        key: loop.run_in_executor(executor, _download_symbol, yf_symbol) for key, yf_symbol in MACRO_SYMBOLS.items()
    }

    # 2) 等待所有 symbol 下载完成
    for key, task in tasks.items():
        try:
            df = await task
        except Exception as e:
            logger.warning(f"Failed to download {key}: {e}")
            continue

        if df is None or df.empty:
            continue

        # 3) 处理 K线
        for ts, row in df.iterrows():
            results.append(
                {
                    "ts": int(ts.timestamp()) * 1000,
                    "dt": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": key,
                    "open": float(row["Open"].iloc[0]),
                    "high": float(row["High"].iloc[0]),
                    "low": float(row["Low"].iloc[0]),
                    "close": float(row["Close"].iloc[0]),
                    "volume": float(row["Volume"].iloc[0]),
                    "source": "yfinance",
                }
            )

    return results


if __name__ == "__main__":
    import time

    async def main():
        results = await get_macro_klines()
        await get_stream_loader().send_rows(results, "macro_kline_raw_1m")

    start = time.time()
    asyncio.run(main())
    end = time.time()
