# data_pipeline/tests/custom_operators/test_data_quality_operator.py


import uuid
import datetime
from pathlib import Path
import pytest
from pytest import raises
from psycopg2.extras import RealDictCursor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helpers.configs.postgres.postgres_config import PostgresConfig
from helpers.lib.sql_queries import SqlQueries
from plugins.custom_operators.transform_load_operator import (
    TransformAndLoadOperator
)
from plugins.custom_operators.data_quality_operator import (
    DataQualityCheckOperator
)


class TestDataQualityOperator:
    @pytest.fixture()
    def setUp(self, postgresql):
        yield "resource"
        postgresql.close()

    def test_data_quality_operator(
        self, mocker, postgresql, ports_collection, test_dag,
        tmp_path: Path
    ):
        """Test if data_quality_operator validates the data correctly."""
        # Create PSQL mock object
        mocker.patch.object(
            PostgresHook,
            "get_conn",
            return_value=postgresql
        )

        # Make sure that the source table has an item in it
        pg_hook = PostgresHook()
        conn = pg_hook.get_conn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = TransformAndLoadOperator._format_query(
            SqlQueries.ports_table_insert,
            {"updated_at": datetime.datetime.utcnow()}
        )
        data = {
            'id': 1,
            'countryName': 'Philippines',
            'portName': 'Aleran/Ozamis',
            'unlocode': 'PH ALE',
            'coordinates': '4234N 00135E',
            'staging_id': uuid.uuid4().__str__()
        }
        cursor.execute(query, data)

        # Define the data quality checker
        postgres_master_config = PostgresConfig('postgres_default')
        check_data_quality = DataQualityCheckOperator(
            tables=['ports'],
            postgres_config=postgres_master_config,
            task_id='test_data_quality_operator',
            queries={
                "ports_row_count": SqlQueries.ports_row_count,
                "ports_updated_count": SqlQueries.ports_updated_count
            },
            dag=test_dag
        )

        expected_data = {'ports_row_count': 1, 'ports_updated_count': 1}

        # Execute task and check if it returns the expected numbers
        quality_result = check_data_quality.execute(context={}, testing=True)
        assert expected_data == quality_result

    def test_data_quality_operator_handles_db_error(
        self, mocker, postgresql, ports_collection, test_dag,
        tmp_path: Path
    ):
        """Test if data_quality_operator correctly handles db errors."""
        # Create PSQL mock object
        mocker.patch.object(
            PostgresHook,
            "get_conn",
            return_value=postgresql
        )

        # Make sure that the source table has an item in it
        pg_hook = PostgresHook()
        conn = pg_hook.get_conn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = TransformAndLoadOperator._format_query(
            SqlQueries.ports_table_insert,
            {"updated_at": datetime.datetime.utcnow()}
        )
        data = {
            'id': 1,
            'countryName': 'Philippines',
            'portName': 'Aleran/Ozamis',
            'unlocode': 'PH ALE',
            'coordinates': '4234N 00135E',
            'staging_id': uuid.uuid4().__str__()
        }
        cursor.execute(query, data)

        # Define the data quality checker
        postgres_master_config = PostgresConfig('postgres_default')
        check_data_quality = DataQualityCheckOperator(
            tables=['ports'],
            postgres_config=postgres_master_config,
            task_id='test',
            queries={
                "ports_row_count": SqlQueries.ports_row_count,
                "ports_updated_count": SqlQueries.ports_updated_count
            },
            dag=test_dag
        )

        with raises(Exception) as exc:
            # Close the connection to implicitly raise an error
            conn.close()
            check_data_quality.execute(
                context={}, testing=True
            )
        assert exc.type is Exception

    def test_data_quality_operator_handles_error(
        self, mocker, postgresql, ports_collection, test_dag,
        tmp_path: Path
    ):
        """Test if data_quality_operator correctly handles Exceptions."""
        # Create PSQL mock object
        mocker.patch.object(
            PostgresHook,
            "get_conn",
            return_value=postgresql
        )

        # Make sure that the source table has an item in it
        pg_hook = PostgresHook()
        conn = pg_hook.get_conn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = TransformAndLoadOperator._format_query(
            SqlQueries.ports_table_insert,
            {"updated_at": datetime.datetime.utcnow()}
        )
        data = {
            'id': 1,
            'countryName': 'Philippines',
            'portName': 'Aleran/Ozamis',
            'unlocode': 'PH ALE',
            'coordinates': '4234N 00135E',
            'staging_id': uuid.uuid4().__str__()
        }
        cursor.execute(query, data)

        # Define the data quality checker
        postgres_master_config = PostgresConfig('postgres_default')
        wrong_query = SqlQueries.ports_row_count.replace('ports', 'foo')
        check_data_quality = DataQualityCheckOperator(
            tables=['ports'],
            postgres_config=postgres_master_config,
            task_id='test',
            queries={
                "ports_row_count": wrong_query,
                "ports_updated_count": wrong_query
            },
            dag=test_dag
        )

        with raises(Exception) as exc:
            check_data_quality.execute(
                context={}, testing=True
            )
        assert exc.type is Exception

    def test_data_quality_operator_closes_db_conn_after_use(
        self, mocker, postgresql, ports_collection, test_dag,
        tmp_path: Path
    ):
        """Test if operator will close the database connection after use."""
        # Create PSQL mock object
        mocker.patch.object(
            PostgresHook,
            "get_conn",
            return_value=postgresql
        )
        # Make sure that the source table has an item in it
        pg_hook = PostgresHook()
        conn = pg_hook.get_conn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = TransformAndLoadOperator._format_query(
            SqlQueries.ports_table_insert,
            {"updated_at": datetime.datetime.utcnow()}
        )
        data = {
            'id': 1,
            'countryName': 'Philippines',
            'portName': 'Aleran/Ozamis',
            'unlocode': 'PH ALE',
            'coordinates': '4234N 00135E',
            'staging_id': uuid.uuid4().__str__()
        }
        cursor.execute(query, data)

        # Define the data quality checker
        postgres_master_config = PostgresConfig('postgres_default')
        check_data_quality = DataQualityCheckOperator(
            tables=['ports'],
            postgres_config=postgres_master_config,
            task_id='test',
            queries={
                "ports_row_count": SqlQueries.ports_row_count,
                "ports_updated_count": SqlQueries.ports_updated_count
            },
            dag=test_dag
        )
        check_data_quality.execute(
            context={}, testing=False
        )
        assert conn.closed

    def test_data_quality_operator_raises_error(
        self, mocker, postgresql, ports_collection, test_dag,
        tmp_path: Path
    ):
        """Test if operator raises an error if the validation didn't passed."""
        # Create PSQL mock object
        mocker.patch.object(
            PostgresHook,
            "get_conn",
            return_value=postgresql
        )

        # Define the data quality checker
        postgres_master_config = PostgresConfig('postgres_default')
        check_data_quality = DataQualityCheckOperator(
            tables=['ports'],
            postgres_config=postgres_master_config,
            task_id='test',
            queries={
                "ports_row_count": SqlQueries.ports_row_count,
                "ports_updated_count": SqlQueries.ports_updated_count
            },
            dag=test_dag
        )

        with raises(Exception) as exc:
            check_data_quality.execute(
                context={}, testing=True
            )
        assert exc.type is Exception
