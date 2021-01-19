# data_pipeline/plugins/custom_operators/transform_load_operator.py


import traceback
from typing import Dict, Union
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo.errors import OperationFailure
from psycopg2.errors import OperationalError, UndefinedTable


class TransformAndLoadOperator(BaseOperator):
    """
    Airflow operator that process and transfers data
    from staging db to master db.
    """
    ui_color = "#9c72f7"

    @apply_defaults
    def __init__(
        self, mongo_config, postgres_config, processor, query,
        query_params=None, *args, **kwargs
    ):
        """
        Airflow operator that process and transfers data
        from staging db to master db.

        :param MongoConfig mongo_config:
            instance of MongoConfig class that provides connection
            parameters for Mongo connection.
        :param PostgresConfig postgres_config:
            instance of PostgresConfig class that provides connection
            parameters for Postgres connection.
        :param PortsItemProcessor processor:
            class that is responsible for processing ports item,
            this processor handles dictionary objects.
        :param str query:
            string object that contains sql query for upsertion
        :param Union[None, Dict] query_params:
            parameters to be used on the query provided
        """
        super().__init__(**kwargs)
        self._postgres_conn_id = postgres_config.conn_id
        self._mongo_conn_id = mongo_config.conn_id
        self._mongo_collection = mongo_config.collection
        self._processor = processor
        self._sql_query = query
        if query_params:
            self._sql_query = TransformAndLoadOperator._format_query(
                query=self._sql_query,
                parameters=query_params
            )

    def execute(self, context, testing=False):
        """
        Read all data from mongo db, process it
        and write to postgresql db.

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
            for idx, document in enumerate(ports_collection.find({})):
                document = self._processor.process_item(document)
                staging_id = document.get('_id').__str__()
                document['staging_id'] = staging_id
                document.pop('_id')
                psql_cursor.execute(self._sql_query, document)
            psql_conn.commit()
        except (OperationalError, UndefinedTable, OperationFailure):
            self.log.error("Writting to database FAILED.")
            self.log.error(traceback.format_exc())
            raise Exception("LoadToMasterdbOperator FAILED.")
        except Exception:
            self.log.error(traceback.format_exc())
            raise Exception("LoadToMasterdbOperator FAILED.")
        finally:
            if not testing:
                self.log.info('Closing database connections...')
                psql_conn.close()
                mongo_hook.close_conn()
        self.log.info(f'UPSERTED {idx+1} records into Postgres Database.')
        self.log.info('LoadToMasterdbOperator SUCCESS!')

    @classmethod
    def _format_query(cls, query: str, parameters: Union[None, Dict]) -> Dict:
        """Format all queries with values specified by paramters dictionary."""
        return query.format(**parameters)
