import uuid
import json
import datetime
from pathlib import Path
import pytest
from pytest import raises
from psycopg2.extras import RealDictCursor
from psycopg2.errors import OperationalError, UndefinedTable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helpers.configs.postgres.postgres_config import PostgresConfig
from helpers.lib.sql_queries import SqlQueries
from helpers.configs.mongo.mongo_config import MongoConfig
from helpers.lib.data_processors import PortsItemProcessor
from plugins.custom_operators.transform_load_operator import (
    TransformAndLoadOperator
)
from plugins.custom_operators.save_json_operator import (
    LoadToJsonOperator
)


class TestTransformLoadOperator:
    @pytest.fixture()
    def setUp(self, postgresql):
        yield "resource"
        postgresql.close()

    def test_save_to_json_operator(
        self, mocker, postgresql, ports_collection, test_dag,
        tmp_path: Path
    ):
        """Test if save_to_json_operator saves the file on a specified path"""
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

        # Setup some data, transfer staging data to master
        mongo_staging_config = MongoConfig('mongo_default', 'ports')
        postgres_master_config = PostgresConfig('postgres_default')
        transform_load = TransformAndLoadOperator(
            mongo_config=mongo_staging_config,
            postgres_config=postgres_master_config,
            task_id='test',
            processor=PortsItemProcessor(),
            query=SqlQueries.ports_table_insert,
            query_params={"updated_at": datetime.datetime.utcnow()},
            dag=test_dag
        )

        # Execute task and check if it inserted the data successfully
        transform_load.execute(context={}, testing=True)
        pg_hook = PostgresHook()
        cursor = pg_hook.get_conn().cursor()
        cursor.execute("SELECT COUNT(*) FROM ports;")
        after_result = cursor.fetchone()[0]
        assert after_result > 0

        # Alter tmp_path to forcesively create a path
        tmp_path = tmp_path / 'unknown-path'

        # Execute save_to_json to save the data into json file on tmp_path
        save_to_json = LoadToJsonOperator(
            task_id='export_to_json',
            postgres_config=postgres_master_config,
            query=SqlQueries.select_all_query_to_json,
            path=tmp_path,
            tables=['ports'],
            dag=test_dag
        )
        save_to_json.execute(
            {'execution_date': datetime.datetime(2021, 1, 1)}
        )

        output_path = tmp_path / 'ports_20210101T000000.json'

        expected_data = {
            'ports': [{
                'id': 1,
                'countryName': 'Philippines',
                'portName': 'Aleran/Ozamis',
                'unlocode': 'PH ALE',
                'coordinates': '4234N 00135E'
            }]
        }

        # Read result
        with open(output_path, "r") as f:
            result = json.load(f)

        # Assert
        assert 'ports' in result
        assert result == expected_data

    def test_save_to_json_operator_exception_error(
        self, mocker, postgresql, ports_collection, test_dag,
        tmp_path: Path
    ):
        """Test if save_to_json_operator can handle Exceptions thrown."""
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

        # Setup some data, transfer staging data to master
        mongo_staging_config = MongoConfig('mongo_default', 'ports')
        postgres_master_config = PostgresConfig('postgres_default')
        transform_load = TransformAndLoadOperator(
            mongo_config=mongo_staging_config,
            postgres_config=postgres_master_config,
            task_id='test',
            processor=PortsItemProcessor(),
            query=SqlQueries.ports_table_insert,
            query_params={"updated_at": datetime.datetime.utcnow()},
            dag=test_dag
        )

        # Execute task and check if it inserted the data successfully
        transform_load.execute(context={}, testing=True)
        pg_hook = PostgresHook()
        cursor = pg_hook.get_conn().cursor()
        cursor.execute("SELECT COUNT(*) FROM ports;")
        after_result = cursor.fetchone()[0]
        assert after_result > 0

        # Intentionally replacing with unknown table
        query = SqlQueries.select_all_query_to_json.replace('table', 'foo')

        # Execute save_to_json to save the data into json file on tmp_path
        save_to_json = LoadToJsonOperator(
            task_id='test2',
            postgres_config=postgres_master_config,
            query=query,
            path=tmp_path,
            tables=['ports'],
            dag=test_dag
        )

        with pytest.raises(Exception):
            save_to_json.execute(
                {'execution_date': datetime.datetime(2021, 1, 1)},
                testing=False
            )

    def test_save_to_json_operator_database_error(
        self, mocker, postgresql, ports_collection, test_dag,
        tmp_path: Path
    ):
        """Test if save_to_json_operator can handle errors related to db."""
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

        # Setup some data, transfer staging data to master
        mongo_staging_config = MongoConfig('mongo_default', 'ports')
        postgres_master_config = PostgresConfig('postgres_default')
        transform_load = TransformAndLoadOperator(
            mongo_config=mongo_staging_config,
            postgres_config=postgres_master_config,
            task_id='test',
            processor=PortsItemProcessor(),
            query=SqlQueries.ports_table_insert,
            query_params={"updated_at": datetime.datetime.utcnow()},
            dag=test_dag
        )

        # Execute task and check if it inserted the data successfully
        transform_load.execute(context={}, testing=True)
        pg_hook = PostgresHook()
        cursor = pg_hook.get_conn().cursor()
        cursor.execute("SELECT COUNT(*) FROM ports;")
        after_result = cursor.fetchone()[0]
        assert after_result > 0

        # Execute save_to_json to save the data into json file on tmp_path
        save_to_json = LoadToJsonOperator(
            task_id='test2',
            postgres_config=postgres_master_config,
            query=SqlQueries.select_all_query_to_json,
            path=tmp_path,
            tables=['foo'],
            dag=test_dag
        )
        with raises((UndefinedTable, OperationalError, Exception)):
            # Set testing = False to implicitly close the database connection
            save_to_json.execute(
                {'execution_date': datetime.datetime(2021, 1, 1)},
                testing=False
            )
            save_to_json.execute(
                {'execution_date': datetime.datetime(2021, 1, 1)},
                testing=True
            )

    def test_transform_load_operator_save_to_json_func(
        self, postgresql, tmp_path: Path
    ):
        """Test if save_to_json function can save a json file given cursor."""
        postgres_master_config = PostgresConfig('postgres_default')
        task = LoadToJsonOperator(
            task_id='test2',
            postgres_config=postgres_master_config,
            query=SqlQueries.select_all_query_to_json,
            path=tmp_path,
            tables=['ports']
        )
        expected_data = {
            'id': 1,
            'countryName': 'Philippines',
            'portName': 'Aleran/Ozamis',
            'unlocode': 'PH ALE',
            'coordinates': '4234N 00135E',
            'staging_id': uuid.uuid4().__str__()
        }
        insert_query = SqlQueries.ports_table_insert.format(
            updated_at=datetime.datetime.utcnow()
        )
        select_query = SqlQueries.select_all_query_to_json.format(
            table='ports'
        )

        # Insert the test data
        cursor = postgresql.cursor(cursor_factory=RealDictCursor)
        cursor.execute(insert_query, expected_data)
        postgresql.commit()

        # Execute to store the data in the memory pool
        cursor.execute(select_query)

        # Save to json file and assert the file data
        file = tmp_path / 'ports.json'
        task.save_to_json(cursor=cursor, key='ports', file_name=file)
        with open(file, "r") as f:
            result = json.load(f)
            assert 'ports' in result
            result = result['ports'][0]

        # Assert expectations
        expected_data.pop('staging_id')
        assert result == expected_data

    def test_transform_load_operator_save_to_json_func_handles_error(
        self, postgresql, tmp_path: Path
    ):
        """Test if save_to_json function can handle Expections thrown."""
        postgres_master_config = PostgresConfig('postgres_default')
        task = LoadToJsonOperator(
            task_id='test2',
            postgres_config=postgres_master_config,
            query=SqlQueries.select_all_query_to_json,
            path=tmp_path,
            tables=['ports']
        )
        cursor = postgresql.cursor()
        file = tmp_path / 'foo.json'
        with raises(Exception):
            task.save_to_json(cursor=cursor, key='foo', file_name=file)
