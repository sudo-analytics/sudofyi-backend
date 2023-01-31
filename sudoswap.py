from sqlalchemy import (
    create_engine,
)
from models import collections, pools, collections_timeseries
import os


class SudoSwapService:
    def __init__(self):
        _engine = create_engine(os.environ["POSTGRES_JBDC"], echo=False)
        self.conn = _engine.connect()

    def get_all_collections(self):
        query = collections.select()
        return self.conn.execute(query)

    def get_pools(self):
        query = pools.select()
        return self.conn.execute(query)

    def get_collection(self, id):
        query = collections.select().where(collections.c.collection_id == id)
        res = self.conn.execute(query)
        return res.fetchone()

    def get_pools_for_collection(self, id):
        query = pools.select().where(pools.c.collection_id == id)
        return self.conn.execute(query)

    def get_timeseries(self, id):
        query = collections_timeseries.select().where(
            collections_timeseries.c.collection_id == id)
        return self.conn.execute(query)
