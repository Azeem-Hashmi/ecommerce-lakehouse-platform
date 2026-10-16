from pyspark.sql import functions as F

from src.logger import get_logger


logger = get_logger(__name__)


def get_processed_files(spark, dataset_name: str) -> set[str]:
    """
    Return source files that were successfully processed
    for a given dataset.
    """

    state_df = spark.table(
        "ecommerce_lakehouse.bronze.ingestion_state"
    )

    processed_files = (
        state_df
        .filter(
            (F.col("dataset_name") == dataset_name)
            & (F.col("status") == "SUCCESS")
        )
        .select("source_file")
        .distinct()
        .collect()
    )

    return {
        row["source_file"]
        for row in processed_files
    }


def get_new_files(
    spark,
    source_path: str,
    dataset_name: str,
) -> list[str]:
    """
    Discover files in the source location that have not
    been successfully processed yet.
    """

    logger.info(f"Checking for new files: {source_path}")

    processed_files = get_processed_files(
        spark=spark,
        dataset_name=dataset_name,
    )

    files = spark.read.format("binaryFile").load(source_path)

    available_files = [
        row.path
        for row in files.select("path").collect()
    ]

    new_files = [
        file_path
        for file_path in available_files
        if file_path not in processed_files
    ]

    logger.info(
        f"Available files: {len(available_files)} | "
        f"Processed files: {len(processed_files)} | "
        f"New files: {len(new_files)}"
    )

    return new_files