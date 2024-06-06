import ccxt
import pandas as pd
import os

# Initialize the Deribit exchange
exchange = ccxt.deribit()

# List of markets to fetch data for
markets = [
    'ETH/USD:ETH-250328-6000-P',
    'ETH/USD:ETH-250328-6500-C',
    'ETH/USD:ETH-250328-6500-P',
    'ETH/USD:ETH-250328-7000-C',
    'ETH/USD:ETH-250328-7000-P',
    'ETH/USD:ETH-250328-7500-C',
    'ETH/USD:ETH-250328-7500-P',
    'ETH/USD:ETH-250328-8000-C',
    'ETH/USD:ETH-250328-8000-P',
    'ETH/USD:ETH-250328-9000-C',
    'ETH/USD:ETH-250328-9000-P',
    'ETH/USD:ETH-250328-10000-C',
    'ETH/USD:ETH-250328-10000-P',
    'ETH/USD:ETH-250328-11000-C',
    'ETH/USD:ETH-250328-11000-P',
    'ETH/USD:ETH-250328-12000-C',
    'ETH/USD:ETH-250328-12000-P',
    'ETH/USD:ETH-250328-15000-C',
    'ETH/USD:ETH-250328-15000-P',
    'BTC-FS-28MAR25_PERP',
    'ETH-FS-28MAR25_PERP',
    'BTC-CS-28MAR25-90000_130000',
    'ADA/USDC:USDC',
    'ALGO/USDC:USDC',
    'AVAX/USDC:USDC',
    'BCH/USDC:USDC',
    'BTC/USD:BTC',
    'BTC/USDC:USDC',
    'BTC/USDT:USDT',
    'DOGE/USDC:USDC',
    'DOT/USDC:USDC',
    'ETH/USD:ETH',
    'ETH/USDC:USDC',
    'ETH/USDT:USDT',
    'LINK/USDC:USDC',
    'LTC/USDC:USDC',
    'MATIC/USDC:USDC',
    'NEAR/USDC:USDC',
    'SOL/USDC:USDC',
    'TRX/USDC:USDC',
    'UNI/USDC:USDC',
    'XRP/USDC:USDC'
]

# Define the timeframe and start date
timeframe = '1m'  # 1 minute
since = exchange.parse8601('2024-06-01T00:00:00Z')  # Start time in ISO8601 format

# Create a directory to save CSV files
output_dir = 'historical_data'
os.makedirs(output_dir, exist_ok=True)

for symbol in markets:
    try:
        # Fetch historical data
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since)

        # Check if data is received
        if not ohlcv:
            print(f"No data received for {symbol}")
            continue

        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # Convert timestamp to a readable format
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Define the output file path
        output_file = os.path.join(output_dir, f"{symbol.replace('/', '_').replace(':', '_')}.csv")

        # Save the data to a CSV file
        df.to_csv(output_file, index=False)
        print(f"Data saved for {symbol} to {output_file}")

    except ccxt.BaseError as e:
        print(f"An error occurred for {symbol}: {e}")
