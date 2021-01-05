import datetime
import traceback
from typing import Callable
import psycopg2
from psycopg2.errors import OperationalError
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import OperationFailure
from helpers.lib.sql_queries import SqlQueries


class LoadToMasterdbOperator(BaseOperator):
    """
    Airflow operator that transfers data from staging db to master db.
    """
    ui_color = "#705B74"
    ui_fgcolor = "#8FA48B"

    @apply_defaults
    def __init__(
        self,
        mongo_conn_uri_key,
        mongo_collection,
        mongo_db,
        postgres_conn_uri_key,
        postgres_table,
        *args,
        **kwargs
    ):
        super().__init__(**kwargs)
        self._postgres_conn_uri = Variable.get(postgres_conn_uri_key)
        self._postgres_table = postgres_table
        self._mongo_conn_uri = Variable.get(mongo_conn_uri_key)
        self._mongo_db = mongo_db
        self._mongo_collection = mongo_collection

    def execute(self, context):
        """
        Read all data from mongo db and write to postgresql db.
        """
        self.log.info('LoadToMasterdbOperator Starting...')
        self.log.info("Initializing Mongo DB Connection...")
        mongo_conn = MongoClient(self._mongo_conn_uri)
        ports_collection = self.get_mongodb_collection(mongo_conn)
        psql_conn = self.connect_to_master_db()
        psql_cursor = psql_conn.cursor()
        ports_table_insert = SqlQueries.ports_table_insert.format(
            updated_at=datetime.datetime.utcnow()
        )

        self.log.info("Loading Staging data to Master Database...")

        try:
            for document in ports_collection.find():
                staging_id = document.get('_id').__str__()
                document['staging_id'] = staging_id
                if staging_id != 'None':
                    document.pop('_id')
                psql_cursor.execute(
                    ports_table_insert,
                    document
                )
            self.log.info("Comitting changes...")
            psql_conn.commit()
        except (OperationalError, OperationFailure):
            self.log.error("Writting to database failed.")
            raise Exception("LoadToMasterdbOperator failed.")
        except Exception:
            self.log.error(traceback.format_exc())
            raise Exception("LoadToMasterdbOperator failed.")
        finally:
            self.log.info("Closing connections...")
            psql_conn.close()
            mongo_conn.close()
        self.log.info('LoadToMasterdbOperator Success!')

    def get_mongodb_collection(self, _mongo_conn) -> Collection:
        """Get mongodb collection from an open MongoClient."""
        _db = _mongo_conn[self._mongo_db]
        _collection = _db[self._mongo_collection]
        return _collection

    def connect_to_master_db(self) -> Callable:
        """Connects to Postgresql database."""
        self.log.info("Initializing PostgreSQL DB Connection...")
        try:
            _psql_conn = psycopg2.connect(self._postgres_conn_uri)
        except OperationalError:
            self.log.error(
                f"Can't connect to PostgreSQL: {self._postgres_conn_uri}"
            )
            raise ValueError(
                "LoadToMasterdbOperator failed. "
                "Cannot connect to PostgresSQL."
            )
        return _psql_conn
