from app.universal_downloader import UniversalOHLCVDownloader
import datetime as dt
import asyncio
import os
from pathlib import Path

def convert_date_to_timestamp(date_str: str) -> int:
    return 1000 * int(dt.datetime.strptime(date_str, "%Y/%m/%d %H:%M %z").timestamp())

exchanges = {
    # "bybit": {"exchange": "bybit", "type": "spot"},
    "bybit_perpetual": {"exchange": "bybit", "type": "future"},
}

pairs = [
    # "BTC/USDT",
    # "ETH/USDT",
    # "XRP/USDT",
    # "SOL/USDT",
    # "FARTCOIN/USDT",
    # "1000PEPE/USDT",
    # "1000X/USDT",
    # "AUCTION/USDT",
    # "ADA/USDT",
    # "DOGE/USDT",
    # "SUI/USDT",
    # "BNB/USDT",
    # "TUT/USDT",
    # "TRUMP/USDT",
    # "TON/USDT",
    # "LINK/USDT",
    # "AVAX/USDT",
    # "WIF/USDT",
    # "ARC/USDT",
    # "HYPE/USDT",
    # "BROCCOLI/USDT",
    # "VIDT/USDT",
    # "ENA/USDT",
    # "POPCAT/USDT",
    # "PENGU/USDT",
    # "LTC/USDT",
    # "ONDO/USDT",
    # "ZEREBRO/USDT",
    # "API3/USDT",
    # "BANANA/USDT",
    # "AAVE/USDT",
    # "CAKE/USDT",
    # "OM/USDT",
    # "EOS/USDT",
    # "MAVIA/USDT",
    # "MAJOR/USDT",
    # "1000BONK/USDT",
    # "GMT/USDT",
    # "NOT/USDT",
    # "AI16Z/USDT",
    # "ZRO/USDT",
    # "DOT/USDT",
    # "NEAR/USDT",
    # "MATIC/USDT",
    # "UNI/USDT",
    # "HBAR/USDT",
    # "TAO/USDT",
    # "ATOM/USDT",
    # "JUP/USDT",
    # "GALA/USDT",
    # "TRX/USDT",
    # "INJ/USDT",
    # "KAS/USDT",
    # "APT/USDT",
    # "ARB/USDT",
    # "SEI/USDT",
    # "OP/USDT",
    "RUNE/USDT"
]

timeframes = [
    "1m",
    # "15m",
    # "1h",
    # "1d"
]

async def fetch_and_save_ohlcv(downloader: UniversalOHLCVDownloader, pair: str, timeframe: str, start_timestamp: int, end_timestamp: int, exchange: str, output_dir: str):
    BASE_DIR = Path(__file__).resolve().parent.parent
    target_dir = f'{BASE_DIR}/{output_dir}/{exchange}'
    try:
        ohlcv_df = await downloader.fetch_ohlcv(pair, timeframe, start_timestamp, end_timestamp)
        # Ensure the directory exists
        os.makedirs(target_dir, exist_ok=True)
        ohlcv_df.to_csv(f'{target_dir}/ohlcv_{exchange}_{pair.replace("/", "-")}_{timeframe}.csv', index=False, compression='gzip')
        print(f"Saved OHLCV data for {exchange} {pair} {timeframe}")
    except Exception as e:
        print(f"Failed to fetch OHLCV data for {exchange} {pair} {timeframe}: {str(e)}")
    


async def main():
    start_timestamp = convert_date_to_timestamp("2024/01/01 00:00 +0000")
    end_timestamp = convert_date_to_timestamp("2025/03/01 00:00 +0000")
    output_dir = "ohlcv_output"
    
    jobs = []
    downloaders = []

    for exchange, config in exchanges.items():
        downloader = UniversalOHLCVDownloader(config["exchange"], config["type"])
        downloaders.append(downloader)
        for pair in pairs:
            for timeframe in timeframes:
                jobs.append(fetch_and_save_ohlcv(downloader, pair, timeframe, start_timestamp, end_timestamp, exchange, output_dir))
        
    await asyncio.gather(*jobs)
    for downloader in downloaders:
        await downloader.exchange.close()

    print("ALL DOWNLOADING TASKS COMPLETED")


if __name__ == "__main__":
    asyncio.run(main())
