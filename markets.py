import ccxt

# Initialize the Deribit exchange
exchange = ccxt.deribit()

# Fetch and print available markets
markets = exchange.load_markets()
for symbol in markets:
    print(symbol)