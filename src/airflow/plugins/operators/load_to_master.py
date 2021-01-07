import datetime
import traceback
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.hooks.postgres_hook import PostgresHook
from pymongo.errors import OperationFailure
from psycopg2.errors import OperationalError
from helpers.lib.sql_queries import SqlQueries


class LoadToMasterdbOperator(BaseOperator):
    """
    Airflow operator that transfers data from staging db to master db.
    """
    ui_color = "#9c72f7"

    @apply_defaults
    def __init__(self, mongo_config, postgres_config, *args, **kwargs):
        """
        Airflow operator that transfers data from staging db to master db.

        :param MongoConfig mongo_config:
            instance of MongoConfig class that provides connection
            parameters for Mongo connection.
        :param PostgresConfig postgres_config:
            instance of PostgresConfig class that provides connection
            parameters for Postgres connection.
        """
        super().__init__(**kwargs)
        self._postgres_conn_id = postgres_config.conn_id
        self._mongo_conn_id = mongo_config.conn_id
        self._mongo_collection = mongo_config.collection

    def execute(self, context):
        """
        Read all data from mongo db and write to postgresql db.
        """
        self.log.info('LoadToMasterdbOperator Starting...')
        self.log.info("Initializing Mongo Staging DB Connection...")
        mongo_hook = MongoHook(conn_id=self._mongo_conn_id)
        ports_collection = mongo_hook.get_collection(
            self._mongo_collection
        )
        self.log.info("Initializing Postgres Master DB Connection...")
        psql_hook = PostgresHook(postgres_conn_id=self._postgres_conn_id)
        psql_conn = psql_hook.get_conn()
        psql_cursor = psql_conn.cursor()
        ports_table_insert = SqlQueries.ports_table_insert.format(
            updated_at=datetime.datetime.utcnow()
        )
        self.log.info("Loading Staging data to Master Database...")
        try:
            print(ports_collection.count_documents({}))
            for document in ports_collection.find({}):
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
            self.log.info('Closing database connections...')
            psql_conn.close()
        self.log.info('LoadToMasterdbOperator Success!')
