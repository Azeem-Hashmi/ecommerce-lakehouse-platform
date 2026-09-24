from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Laptop", 2500),
    (2, "Desktop", 3000),
    (3, "Phone", 1500)
    ]

columns = ["product_id", "product_name", "price"]

df = spark.createDataFrame(data, columns)

df.show()

print("Successfully executed the test Pyspark script on Databricks from VS Code.")



