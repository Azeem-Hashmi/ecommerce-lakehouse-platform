from src.config import load_config


config = load_config()

print("Project:", config["project"]["name"])
print("Environment:", config["environment"]["name"])
print("Catalog:", config["databricks"]["catalog"])
print("Bronze schema:", config["schemas"]["bronze"])
print("Silver schema:", config["schemas"]["silver"])
print("Gold schema:", config["schemas"]["gold"])
print("Log level:", config["logging"]["level"])