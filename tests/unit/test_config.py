from src.config import load_config


config = load_config()

print("Project:", config["project"]["name"])
print("Environment:", config["environment"]["name"])
print("Catalog:", config["databricks"]["catalog"])
print("Bronze schema:", config["schemas"]["bronze"])
print("Silver schema:", config["schemas"]["silver"])
print("Gold schema:", config["schemas"]["gold"])
print("Log level:", config["logging"]["level"])

assert "ingestion" in config
assert "orders" in config["ingestion"]

orders_config = config["ingestion"]["orders"]

assert orders_config["source_folder"] == "orders"
assert orders_config["source_file"] == "orders.csv"
assert orders_config["target_table"] == "orders"
assert orders_config["source_system"] == "sftp"
assert orders_config["ingestion_type"] == "incremental"

print("Ingestion configuration loaded successfully.")