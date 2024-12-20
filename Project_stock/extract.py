import requests
import pandas as pd

# API configuration
API_KEY = "QRSAU3NM5G8QT8W6"  # Replace with your actual API key
BASE_URL = "https://www.alphavantage.co/query"


def fetch_stock_data(symbol, interval="5min"):
    """
    Fetch stock data for a given symbol and interval from Alpha Vantage API.

    :param symbol: Stock symbol to fetch data for.
    :param interval: Time interval between data points (default is '5min').
    :return: DataFrame containing the stock data, or None if an error occurs.
    """
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "apikey": API_KEY
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()

        # Extract the "Time Series" data
        time_series_key = f"Time Series ({interval})"
        time_series = data.get(time_series_key, {})

        # Process the data into a DataFrame
        if not time_series:
            print(f"No data found for symbol: {symbol}")
            return None

        formatted_data = [
            {
                "timestamp": key,
                "open": float(value["1. open"]),
                "high": float(value["2. high"]),
                "low": float(value["3. low"]),
                "close": float(value["4. close"]),
                "volume": int(value["5. volume"])
            }
            for key, value in time_series.items()
        ]
        df = pd.DataFrame(formatted_data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


# Main Execution Block
if __name__ == "__main__":
    symbol = input("Enter the stock symbol (e.g., IBM): ").strip().upper()
    interval = "5min"  # Set your desired interval here

    # Fetch data
    stock_data = fetch_stock_data(symbol, interval)

    if stock_data is not None:
        print(f"Fetched {len(stock_data)} rows of data for {symbol}.")
        print(stock_data.head())  # Display the first few rows

        # Optionally save the data to a CSV file
        stock_data.to_csv(f"{symbol}_intraday_data.csv", index=False)
        print(f"Data saved to {symbol}_intraday_data.csv.")
