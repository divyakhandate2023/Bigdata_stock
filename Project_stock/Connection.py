import yaml
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Load configuration from YAML file
with open('proj.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Extract Cassandra configuration
cassandra_config = config['cassandra']
secure_connect_bundle = cassandra_config['secure_connect_bundle']
client_id = cassandra_config['client_id']
client_secret = cassandra_config['client_secret']

# Define the path to the secure connect bundle and authentication details
cloud_config = {'secure_connect_bundle': secure_connect_bundle}
auth_provider = PlainTextAuthProvider(client_id, client_secret)

# Initialize the cluster connection
try:
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    print("Connection to Cassandra established successfully!")
except Exception as e:
    print(f"Error connecting to Cassandra: {e}")
