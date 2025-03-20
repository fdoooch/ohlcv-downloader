import pandas as pd
import ccxt.async_support as ccxt
import time

class UniversalOHLCVDownloader:

    def __init__(self, exchange_name: str, exchange_type: str = "spot"):
        self.ohlcv_source = exchange_name
        self.exchange: ccxt.Exchange = self._initialize_exchange(exchange_name, exchange_type)

    def _initialize_exchange(self, exchange_name: str, exchange_type: str) -> ccxt.Exchange:
        """Initialize the CCXT exchange instance."""
        try:
            # Check if the exchange is supported by CCXT
            if exchange_name not in ccxt.exchanges:
                raise ValueError(f"Exchange '{exchange_name}' is not supported by CCXT.")
            
            # Create the exchange instance
            exchange_class = getattr(ccxt, exchange_name)
            exchange = exchange_class({
                'enableRateLimit': True,  # Enable built-in rate limiter
            })
            exchange.options['defaultType'] = exchange_type
            return exchange

        except Exception as e:
            raise Exception(f"Failed to initialize exchange {exchange_name}: {str(e)}")

    def _get_timeframe(self, interval: str) -> str:
        """Convert the interval to the exchange-specific timeframe format."""
        return interval

    async def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        start_time: int = None,
        end_time: int = None
    ) -> pd.DataFrame:
        """Fetch OHLCV data using CCXT.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            interval: Time interval (e.g., '1h', '1d')
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            
        Returns:
            DataFrame with OHLCV data
        """
        try:
            # Convert interval to exchange timeframe format
            timeframe = self._get_timeframe(interval)
            
            # Check if the exchange supports fetching OHLCV data
            if not self.exchange.has['fetchOHLCV']:
                raise ValueError(f"Exchange {self.ohlcv_source} does not support fetching OHLCV data")
            
            # Initialize an empty list to store all candles
            all_candles = []
            
            # Set the current timestamp to start_time or use current time if not provided
            since = start_time or int(time.time() * 1000)
            end_time = end_time or int(time.time() * 1000)
            
            # Most exchanges limit the number of candles per request
            # Adjust based on the exchange - some allow 1000, others 500
            limit = 500
            
            print(f"Fetching OHLCV data for {symbol} from {pd.to_datetime(since, unit='ms')} to {pd.to_datetime(end_time, unit='ms')}")
            
            fetch_count = 0
            
            while since < end_time:
                fetch_count += 1
                # print(f"Fetch #{fetch_count}: since={pd.to_datetime(since, unit='ms')}")
                
                # Fetch OHLCV data
                candles = await self.exchange.fetch_ohlcv(
                    symbol=symbol,
                    timeframe=timeframe,
                    since=since,
                    limit=limit,
                )
                
                if not candles or len(candles) == 0:
                    print("No more candles returned from exchange")
                    break
                
                # print(f"Received {len(candles)} candles from {pd.to_datetime(candles[0][0], unit='ms')} to {pd.to_datetime(candles[-1][0], unit='ms')}")
                all_candles.extend(candles)

                since = candles[-1][0]           
                # Respect rate limits
                await self.exchange.sleep(self.exchange.rateLimit / 1000)
                
                # Break if we've reached the end_time or if we got fewer candles than the limit
                if len(candles) < limit:
                    # print("Received fewer candles than limit, assuming we've reached the end")
                    break
                
                # Safety check to prevent infinite loops
                if fetch_count >= 100:  # Arbitrary limit to prevent infinite loops
                    # print("Reached maximum fetch count (100), stopping to prevent infinite loop")
                    break
            
            ohlcv_df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            ohlcv_df = ohlcv_df[ohlcv_df['timestamp'] >= start_time]
            ohlcv_df = ohlcv_df[ohlcv_df['timestamp'] <= end_time]
            
            # Remove duplicates based on timestamp
            ohlcv_df = ohlcv_df.drop_duplicates(subset='timestamp')

            # Sort by timestamp
            ohlcv_df = ohlcv_df.sort_values(by='timestamp')
            
            # print(f"Total candles after filtering and removing duplicates: {len(ohlcv_df)}")
            
            return ohlcv_df
            
        except Exception as e:
            raise Exception(f"Error fetching OHLCV data: {str(e)}")
            
    def _get_timeframe_ms(self, timeframe: str) -> int:
        """Convert a timeframe string to milliseconds."""
        unit = timeframe[-1]
        value = int(timeframe[:-1])
        
        if unit == 'm':
            return value * 60 * 1000
        elif unit == 'h':
            return value * 60 * 60 * 1000
        elif unit == 'd':
            return value * 24 * 60 * 60 * 1000
        elif unit == 'w':
            return value * 7 * 24 * 60 * 60 * 1000
        elif unit == 'M':
            return value * 30 * 24 * 60 * 60 * 1000  # Approximation
        else:
            raise ValueError(f"Unknown timeframe unit: {unit}")