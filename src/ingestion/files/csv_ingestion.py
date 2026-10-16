from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from uuid import uuid4
from src.logger import get_logger


logger = get_logger(__name__)


def read_csv(
    spark,
    source_path: str,
    header: bool = True,
    infer_schema: bool = False,
    schema=None,
) -> DataFrame:
    """
    Read a CSV file from the configured source location.
    """

    logger.info(f"Reading CSV source: {source_path}")

    reader = (
        spark.read
        .option("header", header)
        .option("inferSchema", infer_schema)
    )

    if schema is not None:
        reader = reader.schema(schema)

    df = reader.csv(source_path)

    logger.info(f"CSV read completed. Columns: {len(df.columns)}")

    return df


def add_ingestion_metadata(
    df: DataFrame,
    source_system: str,
) -> tuple[DataFrame, str]:
    """
    Add technical metadata required for ingestion auditing.
    """

    ingestion_id = str(uuid4())

    df = (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_id", F.lit(ingestion_id))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_system", F.lit(source_system))
    )
    
    return df, ingestion_id


def ingest_bronze(
    df: DataFrame,
    target_table: str,
    mode: str = "append",
) -> None:
    """
    Ingest a DataFrame to a Bronze Delta table.
    """

    logger.info(f"Ingesting Bronze table: {target_table}")
    logger.info(f"Ingest mode: {mode}")

    (
        df.write
        .format("delta")
        .mode(mode)
        # .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    logger.info(f"Successfully ingested Bronze table: {target_table}")


def record_ingestion_state(
    spark,
    dataset_name: str,
    ingestion_id: str,
    source_file: str,
    records_processed: int,
    status: str,
) -> None:
    """
    Record the outcome of an ingestion run.
    """

    state_data = [
        (
            dataset_name,
            ingestion_id,
            source_file,
            records_processed,
            status,
        )
    ]

    state_df = spark.createDataFrame(
        state_data,
        [
            "dataset_name",
            "ingestion_id",
            "source_file",
            "records_processed",
            "status",
        ],
    )

    state_df = state_df.withColumn(
        "ingestion_timestamp",
        F.current_timestamp(),
    )

    (
        state_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "ecommerce_lakehouse.bronze.ingestion_state"
        )
    )

    logger.info(
        f"Ingestion state recorded: "
        f"{dataset_name} | {status}"
    )