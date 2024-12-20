# clean.py
import pandas as pd


def clean_data(raw_data):
    """
    Clean and validate raw stock data.
    """
    try:
        # Drop missing values
        cleaned_data = raw_data.dropna()

        # Convert data types
        cleaned_data = cleaned_data.astype({
            'open': float,
            'high': float,
            'low': float,
            'close': float,
            'volume': int,
            'symbol': str
        })

        cleaned_data['date'] = pd.to_datetime(cleaned_data['date'], errors='coerce')
        invalid_dates = cleaned_data['date'].isna().sum()
        print(f"Dropped {invalid_dates} rows with invalid dates.")

        cleaned_data = cleaned_data.dropna(subset=['date'])
        cleaned_data.reset_index(drop=True, inplace=True)

        return cleaned_data
    except Exception as e:
        print(f"Error cleaning data: {e}")
        raise
# load.py
def insert_raw_data(session, table_name, data):
    """
    Insert raw data into the Cassandra `stocks` table.
    """
    query = """
    INSERT INTO stocks (symbol, date, open_price, high_price, low_price, close_price, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for _, row in data.iterrows():
        session.execute(query, (
            row['symbol'], row['date'], row['open'], row['high'], row['low'], row['close'], row['volume']
        ))
    print(f"Raw data inserted into {table_name}.")

def insert_cleaned_data(session, table_name, data):
    """
    Insert cleaned data into the `silver_stock` table.
    """
    query = """
    INSERT INTO silver_stock (symbol, date, open_price, high_price, low_price, close_price, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for _, row in data.iterrows():
        session.execute(query, (
            row['symbol'], row['date'], row['open'], row['high'], row['low'], row['close'], row['volume']
        ))
    print(f"Cleaned data inserted into {table_name}.")
