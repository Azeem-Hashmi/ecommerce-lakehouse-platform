from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

table_name = "ecommerce_lakehouse.bronze.test_table"

data = [
    (1, "Laptop", 1200),
    (2, "Phone", 800),
    (3, "Headphones", 150),
]

df = spark.createDataFrame(
    data,
    ["product_id", "product_name", "price"],
)

df.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Successfully created Delta table: {table_name}")

spark.sql(f"DESCRIBE TABLE {table_name}").show()

