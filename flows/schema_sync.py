from prefect import flow, task
from sqlalchemy import create_engine, text, inspect
from typing import Dict, Optional, Type
import logging
from models.tables import Base
from .utils import no_cache_key
import os


@task(name="Create Database Engine", retries=3, retry_delay_seconds=5)
def create_engine_task(
    database_url: str,
    database_name: Optional[str] = None
) -> create_engine:
    """
    Create SQLAlchemy engine with retry logic

    Args:
        database_url: Base database URL
        database_name: Optional database name to append to URL

    Returns:
        SQLAlchemy engine instance
    """
    if database_name:
        full_url = f"{database_url}/{database_name}"
    else:
        full_url = database_url

    return create_engine(full_url, echo=True)


@task(name="Check Database Exists", retries=2)
def check_database_exists(engine: create_engine, db_name: str) -> bool:
    """
    Check if database exists

    Args:
        engine: SQLAlchemy engine
        db_name: Name of database to check

    Returns:
        bool: True if database exists, False otherwise
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
            ).fetchone()
            return bool(result)
    except Exception as e:
        logging.error(f"Error checking if database exists: {e}")
        raise


@task(name="Create Database", cache_key_fn=no_cache_key)
def create_database(engine: create_engine, db_name: str) -> None:
    """
    Create database if it doesn't exist

    Args:
        engine: SQLAlchemy engine
        db_name: Name of database to create
    """
    try:
        with engine.connect() as connection:
            connection.execute(text(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{db_name}'
                AND pid <> pg_backend_pid()
            """))
            connection.execute(text("commit"))
            connection.execute(text(f"CREATE DATABASE {db_name}"))
            logging.info(f"Created database {db_name}")
    except Exception as e:
        logging.error(f"Error creating database: {e}")
        raise


@task(name="Get Table Columns", cache_key_fn=no_cache_key)
def get_existing_columns(engine: create_engine, table_name: str) -> Dict:
    """
    Get existing columns for a table

    Args:
        engine: SQLAlchemy engine
        table_name: Name of table to inspect

    Returns:
        Dict of column information
    """
    inspector = inspect(engine)
    if inspector.has_table(table_name):
        return {
            column['name']: column
            for column in inspector.get_columns(table_name)
        }
    return {}


@task(name="Sync Table Columns", cache_key_fn=no_cache_key)
def sync_table_columns(
    engine: create_engine,
    table_class: Type[Base],
    existing_columns: Dict
) -> None:
    """
    Synchronize columns for a table

    Args:
        engine: SQLAlchemy engine
        table_class: SQLAlchemy model class
        existing_columns: Dict of existing columns
    """
    table_name = table_class.__tablename__
    declared_columns = {
        column.name: column
        for column in table_class.__table__.columns
    }

    try:
        with engine.connect() as conn:
            for col_name, column in declared_columns.items():
                if col_name not in existing_columns:
                    column_type = column.type.compile(engine.dialect)
                    nullable = "NULL" if column.nullable else "NOT NULL"
                    default = (
                        f"DEFAULT {column.default.arg}"
                        if column.default
                        else ""
                    )

                    conn.execute(text(
                        f"ALTER TABLE {table_name} "
                        f"ADD COLUMN {col_name} {column_type} "
                        f"{nullable} {default}"
                    ))
                    logging.info(f"Added column {col_name} to {table_name}")

            for col_name in (
                set(existing_columns.keys()) - set(declared_columns.keys())
            ):
                if not existing_columns[col_name].get('primary_key', False):
                    conn.execute(text(
                        f"ALTER TABLE {table_name} DROP COLUMN {col_name}"
                    ))
                    logging.info(
                        f"Removed column {col_name} from {table_name}"
                    )

            conn.commit()
    except Exception as e:
        logging.error(f"Error syncing columns for table {table_name}: {e}")
        raise


@task(name="Sync Single Table", cache_key_fn=no_cache_key)
def sync_table_schema(engine: create_engine, table_class: Type[Base]) -> None:
    """
    Synchronize schema for a single table

    Args:
        engine: SQLAlchemy engine
        table_class: SQLAlchemy model class
    """
    table_name = table_class.__tablename__

    try:
        existing_columns = get_existing_columns(engine, table_name)

        if not existing_columns:
            table_class.__table__.create(engine)
            logging.info(f"Created new table {table_name}")
        else:
            sync_table_columns(engine, table_class, existing_columns)

    except Exception as e:
        logging.error(f"Error syncing table {table_name}: {e}")
        raise


@flow(name="Database Schema Sync")
def sync_database_schema(
    database_url: str,
    db_name: str = "data_app",
    retries: int = 3
) -> None:
    """
    Main flow for synchronizing database schema

    Args:
        database_url: Database connection URL
        db_name: Name of database to sync
        retries: Number of retries for the flow
    """
    logging.info(f"Starting schema sync for database {db_name}")

    initial_engine = create_engine_task(database_url)

    try:
        db_exists = check_database_exists(initial_engine, db_name)
        if not db_exists:
            create_database(initial_engine, db_name)
    finally:
        initial_engine.dispose()

    final_engine = create_engine_task(database_url, db_name)

    try:
        for table_class in Base.__subclasses__():
            sync_table_schema(final_engine, table_class)
        logging.info("Schema sync completed successfully")
    except Exception as e:
        logging.error(f"Schema sync failed: {e}")
        raise
    finally:
        final_engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    DATABASE_URL = os.getenv('DATABASE_URL')

    sync_database_schema(
        database_url=DATABASE_URL,
        db_name="data_app"
    )
