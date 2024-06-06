import ccxt
import pandas as pd
import os

# Initialize the Deribit exchange
exchange = ccxt.deribit()

# Define the trading pair, timeframe, and start date
symbol = 'BTC/USD:BTC-250328-50000-P' # 25 year, 03 month, 28 day, 50,000 strike price
timeframe = '1d'  # 1 hour
since = exchange.parse8601('2024-06-01T00:00:00Z')  # Start time in ISO8601 format

# Create a directory to save CSV files
output_dir = 'historical_data'
os.makedirs(output_dir, exist_ok=True)

def fetch_additional_data(symbol):
    """Fetch additional data such as delta and implied volatility."""
    try:
        ticker = exchange.fetch_ticker(symbol)
        # Print the ticker info for debugging
        print(f"Ticker data fetched for {symbol}: {ticker}")
        delta = ticker['info'].get('greeks', {}).get('delta')
        iv = ticker['info'].get('mark_iv')
        return delta, iv
    except Exception as e:
        print(f"An error occurred while fetching additional data for {symbol}: {e}")
        return None, None

try:
    # Fetch historical data
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since)

    # Check if data is received
    if not ohlcv:
        print(f"No data received for {symbol}")
    else:
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # Fetch delta and implied volatility once
        delta, iv = fetch_additional_data(symbol)

        # Apply delta and implied volatility to all rows
        df['delta'] = delta
        df['implied_volatility'] = iv

        # Convert timestamp to a readable format
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Define the output file path
        output_file = os.path.join(output_dir, f"{symbol.replace('/', '_').replace(':', '_')}.csv")
        
        # Print output file path for verification
        print(f"Saving data to {output_file}")

        # Save the data to a CSV file
        df.to_csv(output_file, index=False)
        print(f"Data saved for {symbol} to {output_file}")

except ccxt.BaseError as e:
    print(f"An error occurred for {symbol}: {e}")
