import yfinance as yf
import clickhouse_connect
import pandas_market_calendars as mcal
import datetime
import time

# Connect to ClickHouse client
client = clickhouse_connect.get_client(host="localhost", port="8123", username="default", password="")

# Define list of tickers
ticker_list = ["AAPL", "MSFT", "NVDA"]

# Iterate through each ticker in the list
for ticker in ticker_list:
    tick = yf.Ticker(ticker)
    
    # Download historical data for the ticker (last 10 years)
    data = tick.history(period='10y').reset_index()
    
    # Iterate through each row in the DataFrame
    for _, row in data.iterrows():
        # Insert the data into ClickHouse database
        # Ensure that the correct column names and values are used
        timestamp = row['Date']
        close_price = row['Close']
        
        # Insert data into ClickHouse (uncomment the following line if ready to use)
        client.insert('Stock_Data', [[timestamp, ticker, close_price]], column_names=['timestamp', 'name', 'price'])
        
        # For debugging, print each row
        #print(timestamp, close_price)

