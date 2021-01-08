import traceback
from typing import Dict, Union
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.hooks.postgres_hook import PostgresHook
from pymongo.errors import OperationFailure
from psycopg2.errors import OperationalError


_DEFAULT_VALUE = 'missing_value'


class TransformAndLoadToMasterdbOperator(BaseOperator):
    """
    Airflow operator that transfers data from staging db to master db.
    """
    ui_color = "#9c72f7"

    @apply_defaults
    def __init__(
        self, mongo_config, postgres_config, query, query_params=None,
        *args, **kwargs
    ):
        """
        Airflow operator that transfers data from staging db to master db.

        :param MongoConfig mongo_config:
            instance of MongoConfig class that provides connection
            parameters for Mongo connection.
        :param PostgresConfig postgres_config:
            instance of PostgresConfig class that provides connection
            parameters for Postgres connection.
        :param str query:
            string object that contains sql query for upsertion
        :param Union[None, Dict] query_params:
            parameters to be used on the query provided
        """
        super().__init__(**kwargs)
        self._postgres_conn_id = postgres_config.conn_id
        self._mongo_conn_id = mongo_config.conn_id
        self._mongo_collection = mongo_config.collection
        self._sql_query = query
        if query_params:
            self._sql_query = self.format_query(
                query=self._sql_query,
                parameters=query_params
            )

    def execute(self, context):
        """
        Read all data from mongo db and write to postgresql db.

        Uses UPSERT SQL query to write data.
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
        self.log.info("Loading Staging data to Master Database...")
        try:
            for document in ports_collection.find({}):
                staging_id = document.get('_id').__str__()
                document['staging_id'] = staging_id
                if staging_id != 'None':
                    document.pop('_id')
                psql_cursor.execute(self._sql_query, document)
            psql_conn.commit()
        except (OperationalError, OperationFailure):
            self.log.error("Writting to database FAILED.")
            raise Exception("LoadToMasterdbOperator FAILED.")
        except Exception:
            self.log.error(traceback.format_exc())
            raise Exception("LoadToMasterdbOperator FAILED.")
        finally:
            self.log.info('Closing database connection...')
            psql_conn.close()
            mongo_hook.close_conn()
        self.log.info('LoadToMasterdbOperator SUCCESS!')

    def format_query(self, query: str, parameters: Union[None, Dict]) -> Dict:
        """Format all queries with values specified by paramters dictionary."""
        return query.format(**parameters)
