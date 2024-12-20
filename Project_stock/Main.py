# main.py
import pandas as pd
from connect import connect_to_cassandra
from extract import fetch_stock_data
from clean import clean_data
from load import insert_raw_data, insert_cleaned_data
from ml_modeling import train_ml_models

if __name__ == "__main__":
    # Connect to Cassandra
    session = connect_to_cassandra('proj.yaml')

    # Fetch stock data
    symbol = input("Enter the stock symbol: ").strip().upper()
    raw_data = fetch_stock_data(symbol)

    if raw_data is not None:
        print("Data fetched successfully.")

        # Insert raw data into stocks
        insert_raw_data(session, "stocks", raw_data)

        # Clean data
        cleaned_data = clean_data(raw_data)

        # Insert cleaned data into silver_stock
        insert_cleaned_data(session, "silver_stock", cleaned_data)

        # Perform ML modeling
        train_ml_models(cleaned_data)
