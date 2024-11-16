from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String)
    product_id = Column(String)
    currency = Column(String, nullable=True)
    quantity = Column(Integer)
    shipping_cost = Column(Float, nullable=True)
    amount = Column(Float)
    channel = Column(String, nullable=True)
    channel_group = Column(String, nullable=True)
    campaign = Column(String, nullable=True)
    date_time = Column(DateTime)


class Inventory(Base):
    __tablename__ = "inventories"
    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String)
    name = Column(String)
    quantity = Column(Integer)
    category = Column(String, nullable=True)
    sub_category = Column(String, nullable=True)
