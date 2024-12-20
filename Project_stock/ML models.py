import pandas as pd
import yaml
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np


class StockModeling:
    def __init__(self, session):
        self.session = session
        self.data = None

    def fetch_data_from_silver(self, table_name, symbol):
        """
        Fetch data for a specific stock symbol from the `silver_stock` table.
        """
        try:
            query = f"""
            SELECT symbol, date, open_price, high_price, low_price, close_price, volume
            FROM {table_name}
            WHERE symbol = %s
            """
            rows = self.session.execute(query, (symbol,))

            data = []
            for row in rows:
                data.append({
                    "symbol": row.symbol,
                    "date": str(row.date),  # Convert Cassandra date to string
                    "open": row.open_price,
                    "high": row.high_price,
                    "low": row.low_price,
                    "close": row.close_price,
                    "volume": row.volume
                })
            self.data = pd.DataFrame(data)
            print(f"Fetched {len(self.data)} rows for symbol '{symbol}' from {table_name}.")
        except Exception as e:
            print(f"Error fetching data for modeling: {e}")
            raise

    def train_models(self):
        """
        Train models using Linear Regression, Random Forest, and Support Vector Machine (SVM).
        """
        try:
            if self.data is None or self.data.empty:
                raise ValueError("No data loaded for modeling. Please fetch data first.")

            print("\nTraining ML Models...")
            # Prepare features and target
            X = self.data[['open', 'high', 'low', 'volume']]
            y = self.data['close']

            # Train-test split
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

            # Linear Regression
            lr = LinearRegression()
            lr.fit(X_train, y_train)
            y_pred_lr = lr.predict(X_test)
            lr_mae = mean_absolute_error(y_test, y_pred_lr)
            lr_rmse = np.sqrt(mean_squared_error(y_test, y_pred_lr))
            print(f"Linear Regression - MAE: {lr_mae:.2f}, RMSE: {lr_rmse:.2f}")

            # Random Forest Regressor
            rf = RandomForestRegressor(n_estimators=100, random_state=42)
            rf.fit(X_train, y_train)
            y_pred_rf = rf.predict(X_test)
            rf_mae = mean_absolute_error(y_test, y_pred_rf)
            rf_rmse = np.sqrt(mean_squared_error(y_test, y_pred_rf))
            print(f"Random Forest    - MAE: {rf_mae:.2f}, RMSE: {rf_rmse:.2f}")

            # Support Vector Machine Regressor
            print("Training SVM model...")
            svr = SVR(kernel='rbf')  # Use Radial Basis Function (RBF) kernel
            svr.fit(X_train, y_train)
            y_pred_svr = svr.predict(X_test)
            svr_mae = mean_absolute_error(y_test, y_pred_svr)
            svr_rmse = np.sqrt(mean_squared_error(y_test, y_pred_svr))
            print(f"SVM               - MAE: {svr_mae:.2f}, RMSE: {svr_rmse:.2f}")

        except Exception as e:
            print(f"Error during modeling: {e}")
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

        # Initialize modeling process
        symbol = input("Enter the stock symbol for modeling: ")
        modeler = StockModeling(session)
        modeler.fetch_data_from_silver('silver_stock', symbol)
        modeler.train_models()

    except Exception as e:
        print(f"Error: {e}")
