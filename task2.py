import warnings
import dateparser
import sys

# Ignore dateparser warnings regarding pytz
warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)

import yfinance as yf
import datetime
from datetime import date
from apscheduler.schedulers.blocking import BlockingScheduler
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client.iciciDB
collection = db.candleData

def task():
    try:
        # Get the current time
        now = datetime.datetime.now()

        # Check if the current time is within the specified range (11:15 AM to 2:15 PM)
        start_time = datetime.datetime.strptime("22:42:00", "%H:%M:%S")
        end_time = datetime.datetime.strptime("22:45:00", "%H:%M:%S")
        if not (start_time.time() <= now.time() <= end_time.time()):
            print("Outside logging window. Skipping data retrieval.")
            return

        # Get the candle data for ICICI bank from Yahoo Finance
        ticker = "ICICIBANK.NS"
        stock = yf.Ticker(ticker)
        
        # Check if the required fields exist in the API response
        if 'regularMarketOpen' in stock.info and 'regularMarketDayHigh' in stock.info:
            Open_price = stock.info['regularMarketOpen']
            Highest_price = stock.info['regularMarketDayHigh']
            Close_price = stock.info.get('regularMarketPreviousClose', None)
            Volume = stock.info.get('regularMarketVolume', None)
            Lowest_price = stock.info.get('regularMarketDayLow', None)
            
            # Get current time in the required format
            time = now.strftime("%H:%M:%S")
            date = now.strftime("%Y-%m-%d")

            # Create a dictionary with the candlestick data
            candlestick = {
                "Date": date,
                "Time": time,
                "Open": Open_price,
                "Close": Close_price,
                "Highest": Highest_price,
                "Lowest": Lowest_price,
                "Volume": Volume
            }

            # Insert the dictionary into MongoDB
            collection.insert_one(candlestick)

            # Print the values inserted into MongoDB
            print(candlestick)
        else:
            print("Error: Required fields not found in API response.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

scheduler = BlockingScheduler()
start_date = date.today()

try:
    print("Scheduler started.")
    # Schedule the function to run every 15 minutes for 5 days from 11:15 AM to 2:15 PM
    for day in range(1, 6):
        scheduler.add_job(task, 'interval', seconds=15, start_date=f"{start_date} 21:56:00", end_date=f"{start_date} 22:56:00")
        scheduler.start()
except (KeyboardInterrupt, SystemExit):
    print("Scheduler has been stopped.")
    sys.exit(0)