from databricks.connect import DatabricksSession


spark = DatabricksSession.builder.getOrCreate() 

print("Spark version:", spark.version)

df = spark.createDataFrame(
    [
        (1, "Laptop", 1200),
        (2, "Phone", 800),
        (3, "Headphones", 150),
    ],
    ["product_id", "product_name", "price"],
)

df.show()

print("Successfully connected to Databricks Serverless!")