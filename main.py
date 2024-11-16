from prefect import flow
from flows.schema_sync import sync_database_schema
from flows.data_ingestion import ingest_data
import os

DATABASE_URL = os.getenv('DATABASE_URL')


@flow(name="Main ETL Flow")
def main_flow(
    orders_file_path: str,
    database_url: str = DATABASE_URL,
    db_name: str = "data_app"
):
    """Main flow that orchestrates schema sync and data ingestion"""

    sync_database_schema(
        database_url=database_url,
        db_name=db_name
    )

    ingest_data(
        orders_file_path="data/orders.csv",
        inventories_file_path="data/inventory.csv",
        database_url=DATABASE_URL
    )


if __name__ == "__main__":
    main_flow(
        orders_file_path="data/orders.csv"
    )
