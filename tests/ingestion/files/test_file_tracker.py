from databricks.connect import DatabricksSession

from src.config import load_config
from src.ingestion.files.file_tracker import get_new_files


def main():
    config = load_config()

    bucket_name = config["storage"]["bucket"]
    source_prefix = config["storage"]["source_prefix"]

    source_path = (
        f"s3://{bucket_name}/{source_prefix}/orders/"
    )

    spark = DatabricksSession.builder.getOrCreate()

    new_files = get_new_files(
        spark=spark,
        source_path=source_path,
        dataset_name="orders",
    )

    print("\nNew files detected:")

    for file_path in new_files:
        print(file_path)

    spark.stop()


if __name__ == "__main__":
    main()