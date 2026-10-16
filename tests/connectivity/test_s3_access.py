from databricks.connect import DatabricksSession


S3_PATH = "s3://ecommerce-lakehouse-platform/sftp-data/orders/orders.csv"


def main():
    spark = DatabricksSession.builder.getOrCreate()

    print("=" * 60)
    print("Testing S3 -> Databricks connectivity")
    print("=" * 60)

    print(f"S3 source: {S3_PATH}")

    orders_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(S3_PATH)
    )

    print(f"\nSchema:")
    orders_df.printSchema()

    print(f"\nSample records:")
    orders_df.show(5, truncate=False)

    print(f"\nRecord count: {orders_df.count()}")

    print(f"\nS3 access test successful.")

    spark.stop()


if __name__ == "__main__":
    main()