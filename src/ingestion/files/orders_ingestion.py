from databricks.connect import DatabricksSession

from src.config import load_config
from src.logger import get_logger

from src.ingestion.files.csv_ingestion import (
    add_ingestion_metadata,
    read_csv,
    ingest_bronze,
    record_ingestion_state,
)

from src.ingestion.files.file_tracker import get_new_files

from src.ingestion.schemas.orders_schema import ORDERS_SCHEMA


logger = get_logger(__name__)


def ingest_orders(spark) -> None:
    """Ingest new orders source files into the Bronze Delta layer."""

    config = load_config()

    bucket_name = config["storage"]["bucket"]
    source_prefix = config["storage"]["source_prefix"]

    catalog = config["databricks"]["catalog"]
    bronze_schema = config["schemas"]["bronze"]

    dataset_name = "orders"
    dataset_config = config["ingestion"][dataset_name]

    source_folder = dataset_config["source_folder"]
    source_system = dataset_config["source_system"]
    ingestion_type = dataset_config["ingestion_type"]
    target_table_name = dataset_config["target_table"]

    source_path = (
        f"s3://{bucket_name}/{source_prefix}/"
        f"{source_folder}/"
    )

    target_table = (
        f"{catalog}.{bronze_schema}.{target_table_name}"
    )

    logger.info("Starting orders incremental ingestion")
    logger.info(f"Source: {source_path}")
    logger.info(f"Target: {target_table}")
    logger.info(f"Ingestion type: {ingestion_type}")

    # 1. Discover files that have not been processed yet.
    new_files = get_new_files(
        spark=spark,
        source_path=source_path,
        dataset_name=dataset_name,
    )

    if not new_files:
        logger.info("No new files found. Nothing to ingest.")
        return

    logger.info(f"New files found: {len(new_files)}")

    # 2. Process each new file independently.
    for source_file in new_files:

        logger.info(f"Processing file: {source_file}")

        # Read source
        orders_df = read_csv(
            spark=spark,
            source_path=source_file,
            header=True,
            infer_schema=False,
            schema=ORDERS_SCHEMA,
        )

        # Add technical metadata
        orders_df, ingestion_id = add_ingestion_metadata(
            orders_df,
            source_system=source_system,
        )

        # Count records
        records_processed = orders_df.count()

        logger.info(
            f"Records processed from {source_file}: "
            f"{records_processed}"
        )

        # Remove duplicate order IDs within the incoming file.
        orders_df = orders_df.dropDuplicates(["order_id"])

        # Find orders that already exist in Bronze.
        if spark.catalog.tableExists(target_table):
            existing_order_ids = (
                spark.table(target_table)
                .select("order_id")
                .distinct()
            )

            new_orders_df = orders_df.join(
                existing_order_ids,
                on="order_id",
                how="left_anti",
            )
        else:
            new_orders_df = orders_df

        # Count records that will actually be inserted.
        new_records_count = new_orders_df.count()

        logger.info(
            f"New orders to insert: {new_records_count}"
        )

        # Append only new orders.
        if new_records_count > 0:
            ingest_bronze(
                df=new_orders_df,
                target_table=target_table,
                mode="append",
            )
        else:
            logger.info("No new orders to insert.")

        # Record successful ingestion
        record_ingestion_state(
            spark=spark,
            dataset_name=dataset_name,
            ingestion_id=ingestion_id,
            source_file=source_file,
            records_processed=records_processed,
            status="SUCCESS",
        )

        logger.info(
            f"Successfully processed: {source_file}"
        )

    logger.info("Orders incremental ingestion completed")


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()

    ingest_orders(spark)

    spark.stop()