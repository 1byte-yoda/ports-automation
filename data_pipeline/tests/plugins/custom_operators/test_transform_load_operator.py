# data_pipeline/tests/custom_operators/test_transform_load_operator.py


import datetime
import pytest
from pytest import raises
from psycopg2.errors import UndefinedTable, OperationalError
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helpers.configs.postgres.postgres_config import PostgresConfig
from helpers.lib.sql_queries import SqlQueries
from helpers.configs.mongo.mongo_config import MongoConfig
from helpers.lib.data_processors import PortsItemProcessor
from plugins.custom_operators.transform_load_operator import (
    TransformAndLoadOperator
)


class TestTransformLoadOperator:
    @pytest.fixture()
    def setUp(self, postgresql):
        yield "resource"
        postgresql.close()

    def test_transform_load_operator(
        self, mocker, postgresql, ports_collection, test_dag
    ):
        """Test if transform_load_operator upserts data into master db."""
        # Create mocks
        mocker.patch.object(
            PostgresHook,
            "get_conn",
            return_value=postgresql
        )
        mocker.patch.object(
            MongoHook,
            "get_collection",
            return_value=ports_collection
        )

        # Check if the source table has an item in it
        mongo_hook = MongoHook()
        collection = mongo_hook.get_collection()
        assert collection.count_documents({}) > 0

        # Check if the sink table is initially empty
        cursor = postgresql.cursor()
        cursor.execute("SELECT COUNT(*) FROM ports;")
        initial_result = cursor.fetchone()[0]
        assert initial_result == 0

        # Setup task
        mongo_staging_config = MongoConfig('mongo_default', 'ports')
        postgres_master_config = PostgresConfig('postgres_default')
        task = TransformAndLoadOperator(
            mongo_config=mongo_staging_config,
            postgres_config=postgres_master_config,
            task_id='test',
            processor=PortsItemProcessor(),
            query=SqlQueries.ports_table_insert,
            query_params={"updated_at": datetime.datetime.utcnow()},
            dag=test_dag
        )

        # Execute task and check if it inserted the data successfully
        task.execute(context={}, testing=True)
        cursor.execute("SELECT COUNT(*) FROM ports;")
        after_result = cursor.fetchone()[0]
        assert after_result > 0

    def test_transform_load_operator_exception_error(
        self, mocker, postgresql, ports_collection, test_dag
    ):
        """Test if transform_load_operator handles Exception thrown."""
        # Create mocks
        mocker.patch.object(
            PostgresHook,
            "get_conn",
            return_value=postgresql
        )
        mocker.patch.object(
            MongoHook,
            "get_collection",
            return_value=ports_collection
        )

        # Check if the source table has an item in it
        mongo_hook = MongoHook()
        collection = mongo_hook.get_collection()
        assert collection.count_documents({}) > 0

        # Setup task
        mongo_staging_config = MongoConfig('mongo_default', 'ports')
        postgres_master_config = PostgresConfig('postgres_default')
        task = TransformAndLoadOperator(
            mongo_config=mongo_staging_config,
            postgres_config=postgres_master_config,
            task_id='test',
            processor=PortsItemProcessor(),
            query='Wrong SQL query',
            dag=test_dag
        )

        # Execute task and check if it will raise an Exception error
        with raises(Exception):
            task.execute(context={}, testing=True)

    def test_transform_load_operator_database_error(
        self, mocker, postgresql, ports_collection, test_dag
    ):
        """Test if transform_load_operator handles DB errors."""
        # Create mocks
        mocker.patch.object(
            PostgresHook,
            "get_conn",
            return_value=postgresql
        )
        mocker.patch.object(
            MongoHook,
            "get_collection",
            return_value=ports_collection
        )

        # Check if the source table has an item in it
        mongo_hook = MongoHook()
        collection = mongo_hook.get_collection()
        assert collection.count_documents({}) > 0

        # Setup task, intentionally give an unknown table
        mongo_staging_config = MongoConfig('mongo_default', 'ports')
        postgres_master_config = PostgresConfig('postgres_default')
        task = TransformAndLoadOperator(
            mongo_config=mongo_staging_config,
            postgres_config=postgres_master_config,
            task_id='test',
            processor=PortsItemProcessor(),
            query=SqlQueries.ports_table_insert.replace(
                'ports', 'ports_wrong'
            ),
            query_params={"updated_at": datetime.datetime.utcnow()},
            dag=test_dag
        )

        # Execute the task and check if it will raise an UndefinedTable error
        with raises((UndefinedTable, Exception, OperationalError)):
            # Set testing to false to implicitly close the database
            task.execute(context={}, testing=False)
            task.execute(context={}, testing=True)
