from sqlalchemy.exc import SQLAlchemyError
import logging
import json
from dune import DuneAnalytics
import os
from models import collections, collections_timeseries, pools
from enum import Enum
from sqlalchemy import (
    create_engine,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker


class Query(Enum):
    COLLECTIONS = 1140117  # https://dune.com/queries/1140117
    POOLS = 1139834  # https://dune.com/queries/1139834
    COLLECTIONS_TIMESERIES = 1140220  # https://dune.com/queries/1140220


def write_to_file(name, data, should_write):
    if should_write:
        f = open(name, "w")
        f.write(json.dumps(data))
        f.close()


def read_from_file(name):
    f = open(name, "r")
    data = f.read()
    f.close()
    return json.loads(data)


class SyncDuneDataJob:

    def __init__(self):
        _engine = create_engine(os.environ["POSTGRES_JBDC"], echo=False)
        self.conn = _engine.connect()

        self.dune = DuneAnalytics(
            username=os.environ["DUNE_USER"], password=os.environ["DUNE_PASS"])
        self.dune.login()
        self.dune.fetch_auth_token()

    def sync(self):
        print("Running sync_collections job")
        self.sync_collections()

        print("Running sync_pools job")
        self.sync_pools()

        print("Running sync_timeseries job")
        self.sync_timeseries()

    def sync_collections(self):
        result_id = self.dune.query_result_id(query_id=Query.COLLECTIONS.value)
        data = self.dune.query_result(result_id)
        rows = data["data"]["get_result_by_result_id"]
        rows = list(map(lambda row: row["data"], rows))

        write_to_file("data/collections.json", rows, False)

        Session = sessionmaker(bind=self.conn)
        session = Session()
        try:
            session.query(collections).delete()
            stmt = insert(collections).values(rows)
            session.execute(stmt)
            session.commit()
        except:
            session.rollback()
            raise

    def sync_pools(self):
        result_id = self.dune.query_result_id(query_id=Query.POOLS.value)
        data = self.dune.query_result(result_id)
        rows = data["data"]["get_result_by_result_id"]
        rows = list(map(lambda row: row["data"], rows))

        write_to_file("data/pools.json", rows, False)

        Session = sessionmaker(bind=self.conn)
        session = Session()
        try:
            session.query(pools).delete()
            stmt = insert(pools).values(rows)
            session.execute(stmt)
            session.commit()
        except:
            session.rollback()
            raise

    def sync_timeseries(self):
        result_id = self.dune.query_result_id(
            query_id=Query.COLLECTIONS_TIMESERIES.value)
        data = self.dune.query_result(result_id)

        rows = data["data"]["get_result_by_result_id"]
        rows = list(map(lambda row: row["data"], rows))

        write_to_file("data/timeseries.json", rows, False)

        stmt = insert(collections_timeseries).values(rows)
        upsert_statement = stmt.on_conflict_do_nothing()

        try:
            self.conn.execute(upsert_statement)
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            print(error)


def sync():
    logging.info("Running sync job")
    job = SyncDuneDataJob()
    job.sync()
