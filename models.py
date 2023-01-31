from logging import getLogger

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    MetaData,
    String,
    Table,
    Integer,
    create_engine,
)
import os


logger = getLogger()

meta = MetaData()

collections = Table(
    f"collections",
    meta,
    Column("collection_id", String, primary_key=True),
    Column("collection_name", String),
    Column("images", String),
    Column("ticker", String),
    Column("floor_price", Float),
    Column("best_offer", Float),
    Column("volume_eth", Float),
    Column("delta_percentage", Float),
    Column("listings_number", Float),
    Column("offers_eth", Float),
    Column("mover_rank", Integer),
)

pools = Table(
    f"pools",
    meta,
    Column("pool_address", String, primary_key=True),
    Column("collection_id", String),
    Column("collection_name", String),
    Column("images", String),
    Column("ticker", String),
    Column("bonding_curve", String),
    Column("bonding_delta", Float),
    Column("swap_fee", Float),
    Column("floor_price", Float),
    Column("best_offer", Float),
    Column("volume_eth", Float),
    Column("delta_percentage", Float),
    Column("listings_number", Float),
    Column("offers_eth", Float),
)

collections_timeseries = Table(
    f"collections_timeseries",
    meta,
    Column("collection_id", String, primary_key=True),
    Column("datetime", DateTime, primary_key=True),
    Column("tx_number", Float),
    Column("txn_price", Float),
    Column("volume_eth", Float),
    Column("listings_number", Float),
    Column("offers_eth", Float),
)


def migrate():
    logger.info("Running migration to create all tables")
    engine = create_engine(os.environ["POSTGRES_JBDC"], echo=False)
    meta.create_all(engine)
