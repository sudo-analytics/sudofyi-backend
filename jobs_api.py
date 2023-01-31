from sqlalchemy.exc import SQLAlchemyError
import logging
import json
import os
from models import collections, collections_timeseries, pools
from enum import Enum
from sqlalchemy import (
    create_engine,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from dune_api import (
    get_query_status,
    execute_query,
    get_query_results
)
import os
import time

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

def get_query_result(query_id):
  execution_id = execute_query(query_id)
  while True:
    response = get_query_status(execution_id)
    state = response.json()['state']
    print("Checking function state")
    print(state)

    if state == "QUERY_STATE_COMPLETED":
      print("Query done")
      break
  
    time.sleep(20)

  response = get_query_results(execution_id)
  print(response.json())
  return response.json()["result"]["rows"]


class SyncDuneDataJob:

    def __init__(self):
        _engine = create_engine(os.environ["POSTGRES_JBDC"], echo=False)
        self.conn = _engine.connect()

    def sync(self):
        print("Running sync_collections job")
        self.sync_collections()

        print("Running sync_pools job")
        self.sync_pools()

        print("Running sync_timeseries job")
        self.sync_timeseries()

    def sync_collections(self):
        rows = get_query_result(str(Query.COLLECTIONS.value))

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
        rows = get_query_result(str(Query.POOLS.value))

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
        rows = get_query_result(str(Query.COLLECTIONS_TIMESERIES.value))

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
