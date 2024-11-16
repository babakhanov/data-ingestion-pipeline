from prefect import flow, task
from sqlalchemy import create_engine, select, and_, or_
from sqlalchemy.orm import Session
import pandas as pd
from typing import List, Dict, Any
import logging
from models.tables import Order, Inventory
from sqlalchemy.dialects.postgresql import insert
from .utils import no_cache_key, camel_to_snake


def get_existing_inventories(
    engine,
    product_ids: List[str]
) -> Dict[str, Dict[str, Any]]:
    """Get existing inventory items by product_id"""
    with Session(engine) as session:
        query = session.execute(
            select(Inventory).where(Inventory.product_id.in_(product_ids))
        ).scalars().all()

        return {
            inventory.product_id: {
                'id': inventory.id,
                'product_id': inventory.product_id,
                'name': inventory.name,
                'quantity': inventory.quantity,
                'category': inventory.category,
                'sub_category': inventory.sub_category
            }
            for inventory in query
        }


def get_existing_orders(
    engine,
    order_product_pairs: List[tuple]
) -> Dict[tuple, Dict[str, Any]]:
    """Get existing orders by a combination of order_id and product_id"""
    with Session(engine) as session:
        conditions = [
            and_(Order.order_id == order_id, Order.product_id == product_id)
            for order_id, product_id in order_product_pairs
        ]

        query = session.execute(
            select(Order).where(or_(*conditions))
        ).scalars().all()

        return {
            (order.order_id, order.product_id): {
                'id': order.id,
                'order_id': order.order_id,
                'product_id': order.product_id,
                'currency': order.currency,
                'quantity': order.quantity,
                'shipping_cost': order.shipping_cost,
                'amount': order.amount,
                'channel': order.channel,
                'channel_group': order.channel_group,
                'campaign': order.campaign,
                'date_time': order.date_time
            }
            for order in query
        }


@task(name="Read Inventories CSV", retries=2)
def read_inventories_csv(file_path: str) -> pd.DataFrame:
    """Read inventory data from CSV file and perform initial data cleaning"""
    df = pd.read_csv(file_path)
    df.columns = [camel_to_snake(col) for col in df.columns]

    if 'product_id' in df.columns:
        df['product_id'] = df['product_id'].astype(str)

    return df


@task(name="Read Orders CSV", retries=2)
def read_orders_csv(file_path: str) -> pd.DataFrame:
    """Read orders from CSV file and perform initial data cleaning"""
    df = pd.read_csv(file_path)
    df.columns = [camel_to_snake(col) for col in df.columns]

    date_columns = ['date_time']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col].str.replace("Z", "+00:00"), format='ISO8601'
            )

    if 'product_id' in df.columns:
        df['product_id'] = df['product_id'].astype(str)

    return df


@task(name="Prepare Inventories for Upsert")
def prepare_inventories_for_upsert(
    df: pd.DataFrame,
    existing_inventories: Dict[str, Dict[str, Any]]
) -> tuple[List[Dict], List[Dict]]:
    """Separate inventories into updates and inserts"""
    updates = []
    inserts = []

    for _, row in df.iterrows():
        inventory_data = {
            key: (None if pd.isna(value) else value)
            for key, value in row.to_dict().items()
        }

        product_id = str(inventory_data['product_id'])

        if product_id in existing_inventories:
            updates.append({
                'id': existing_inventories[product_id]['id'],
                **inventory_data
            })
        else:
            inserts.append(inventory_data)

    return updates, inserts


@task(name="Prepare Orders for Upsert")
def prepare_orders_for_upsert(
    df: pd.DataFrame,
    existing_orders: Dict[tuple, Dict[str, Any]]
) -> tuple[List[Dict], List[Dict]]:
    """Separate orders into updates and inserts"""
    updates = []
    inserts = []

    for _, row in df.iterrows():
        order_data = {
            key: (None if pd.isna(value) else value)
            for key, value in row.to_dict().items()
        }

        order_id = str(order_data['order_id'])
        product_id = str(order_data['product_id'])
        order_key = (order_id, product_id)

        if order_key in existing_orders:
            updates.append({
                'id': existing_orders[order_key]['id'],
                **order_data
            })
        else:
            inserts.append(order_data)

    return updates, inserts


@task(name="Upsert Inventories", cache_key_fn=no_cache_key)
def upsert_inventories(
    engine,
    updates: List[Dict],
    inserts: List[Dict]
) -> None:
    """Update existing inventories and insert new ones"""
    with Session(engine) as session:
        try:
            for update_data in updates:
                inventory_id = update_data.pop('id')
                stmt = (
                    select(Inventory)
                    .where(Inventory.id == inventory_id)
                )
                inventory = session.execute(stmt).scalar_one()

                for key, value in update_data.items():
                    setattr(inventory, key, value)

            for insert_data in inserts:
                stmt = insert(Inventory).values(insert_data)
                session.execute(stmt)

            session.commit()

        except Exception as e:
            session.rollback()
            raise e


@task(name="Upsert Orders", cache_key_fn=no_cache_key)
def upsert_orders(
    engine,
    updates: List[Dict],
    inserts: List[Dict]
) -> None:
    """Update existing orders and insert new ones"""
    with Session(engine) as session:
        try:
            for update_data in updates:
                order_id = update_data.pop('id')
                stmt = (
                    select(Order)
                    .where(Order.id == order_id)
                )
                order = session.execute(stmt).scalar_one()

                for key, value in update_data.items():
                    setattr(order, key, value)

            for insert_data in inserts:
                stmt = insert(Order).values(insert_data)
                session.execute(stmt)

            session.commit()

        except Exception as e:
            session.rollback()
            raise e


@flow(name="Data Ingestion")
def ingest_data(
    orders_file_path: str,
    inventories_file_path: str,
    database_url: str,
    db_name: str = "data_app"
) -> None:
    """Main flow for ingesting orders and inventories data"""

    engine = create_engine(f"{database_url}/{db_name}")

    try:
        df_orders = read_orders_csv(orders_file_path)
        logging.info(f"Read {len(df_orders)} rows from orders CSV")

        order_product_pairs = [
            (str(row['order_id']), str(row['product_id']))
            for _, row in df_orders.iterrows()
        ]
        existing_orders = get_existing_orders(engine, order_product_pairs)

        updates_orders, inserts_orders = prepare_orders_for_upsert(
            df_orders,
            existing_orders
        )
        logging.info(
            f"Prepared {len(updates_orders)} updates and "
            f"{len(inserts_orders)} inserts for orders"
        )
        upsert_orders(engine, updates_orders, inserts_orders)

        df_inventories = read_inventories_csv(inventories_file_path)
        logging.info(f"Read {len(df_inventories)} rows from inventories CSV")

        inventory_product_ids = [
            str(row['product_id']) for _, row in df_inventories.iterrows()
        ]
        existing_inventories = get_existing_inventories(
            engine,
            inventory_product_ids
        )

        updates_inventories, inserts_inventories = (
            prepare_inventories_for_upsert(
                df_inventories,
                existing_inventories
            )
        )

        logging.info(
            f"Prepared {len(updates_inventories)} updates and "
            f"{len(inserts_inventories)} inserts for inventories"
        )
        upsert_inventories(engine, updates_inventories, inserts_inventories)

        logging.info(
            "Successfully completed data ingestion for orders and inventories"
        )

    finally:
        engine.dispose()
