import pandas as pd
import yaml
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class StockDataProcessor:
    def __init__(self, session):
        self.session = session
        self.data = None
        self.cleaned_data = None

    def fetch_stocks_data(self, table_name):
        """Fetch all data from the stocks table in Cassandra."""
        try:
            query = f"SELECT symbol, date, open_price, high_price, low_price, close_price, volume FROM {table_name}"
            rows = self.session.execute(query)

            data = []
            for row in rows:
                data.append({
                    "symbol": row.symbol,
                    "date": row.date,
                    "open": row.open_price,
                    "high": row.high_price,
                    "low": row.low_price,
                    "close": row.close_price,
                    "volume": row.volume
                })
            self.data = pd.DataFrame(data)
            print(f"Fetched {len(self.data)} rows from {table_name}.")
        except Exception as e:
            print(f"Error fetching data from {table_name}: {e}")
            raise

    def clean_data(self):
        """Clean the fetched data and format the date column."""
        try:
            if self.data is None or self.data.empty:
                raise ValueError("No data to clean. Load data before cleaning.")

            # Convert the date column to a proper datetime format
            self.data['date'] = pd.to_datetime(self.data['date'], errors='coerce', format='%Y-%m-%d')
            invalid_dates = self.data['date'].isna().sum()

            # Drop rows with invalid dates
            self.cleaned_data = self.data.dropna(subset=['date']).copy()
            self.cleaned_data.reset_index(drop=True, inplace=True)

            print(f"Cleaned data: {len(self.cleaned_data)} valid rows. Dropped {invalid_dates} rows with invalid dates.")
        except Exception as e:
            print(f"Error cleaning data: {e}")
            raise

    def recreate_silver_table(self, table_name):
        """Drop and recreate the silver_stock table."""
        try:
            self.session.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Dropped existing table {table_name}.")

            create_query = f"""
            CREATE TABLE {table_name} (
                symbol TEXT,
                date DATE,
                open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                close_price FLOAT,
                volume INT,
                PRIMARY KEY (symbol, date)
            )
            """
            self.session.execute(create_query)
            print(f"Recreated table {table_name}.")
        except Exception as e:
            print(f"Error recreating table {table_name}: {e}")
            raise

    def bulk_insert_to_silver(self, table_name):
        """Bulk insert the cleaned data into the silver_stock table."""
        try:
            insert_query = f"""
            INSERT INTO {table_name} (symbol, date, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            batch_size = 100
            batch = []

            for _, row in self.cleaned_data.iterrows():
                batch.append((
                    row['symbol'], row['date'].date(), row['open'], row['high'], row['low'], row['close'], row['volume']
                ))

                # Execute batch when batch size is reached
                if len(batch) >= batch_size:
                    self._execute_batch(batch, insert_query)
                    batch = []

            # Insert remaining rows
            if batch:
                self._execute_batch(batch, insert_query)

            print(f"Bulk inserted data into {table_name}.")
        except Exception as e:
            print(f"Error during bulk insert to {table_name}: {e}")
            raise

    def _execute_batch(self, batch, query):
        """Helper function to execute a batch of inserts."""
        try:
            for params in batch:
                self.session.execute(query, params)
        except Exception as e:
            print(f"Error during batch execution: {e}")
            raise


if __name__ == "__main__":
    # Load YAML configuration
    with open('proj.yaml', 'r') as file:
        config = yaml.safe_load(file)

    cassandra_config = config['cassandra']
    cloud_config = {'secure_connect_bundle': cassandra_config['secure_connect_bundle']}
    auth_provider = PlainTextAuthProvider(cassandra_config['client_id'], cassandra_config['client_secret'])

    try:
        # Connect to Cassandra
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        session.set_keyspace('stock')

        # Initialize processor
        processor = StockDataProcessor(session)

        # Fetch, clean, and insert data
        processor.fetch_stocks_data('stocks')  # Fetch raw data from `stocks`
        processor.clean_data()  # Clean data and correct `date` column
        processor.recreate_silver_table('silver_stock')  # Recreate `silver_stock`
        processor.bulk_insert_to_silver('silver_stock')  # Bulk insert cleaned data to `silver_stock`

    except Exception as e:
        print(f"Error: {e}")
