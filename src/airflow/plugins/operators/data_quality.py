from typing import Callable
import psycopg2
from psycopg2.errors import OperationalError
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from helpers.lib.sql_queries import SqlQueries


class DataQualityCheckOperator(BaseOperator):
    """
    Airflow operator that runs SQL queries to check the quality
    of downstream data.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, tables, postgres_conn_uri_key, *args, **kwargs):
        super().__init__(**kwargs)
        self._tables = tables
        self._postgres_conn_uri = Variable.get(postgres_conn_uri_key)

    def execute(self, context):
        self.log.info('DataQualityCheckOperator Starting...')
        psql_conn = self.connect_to_master_db()
        psql_cursor = psql_conn.cursor()
        try:
            for table in self._tables:
                table_row_count = SqlQueries.table_row_count
                ports_table_data_count = table_row_count.format(
                    table=table
                )
                psql_cursor.execute(ports_table_data_count)
                row_count = psql_cursor.fetchone()[0]
                if row_count < 1:
                    error = (
                        "Data quality check failed. "
                        f"{table} returned no results."
                    )
                    self.log.error(error)
                    raise ValueError(error)
                self.log.info(
                    f"Data quality on table {table} check passed "
                    f"with {row_count} records."
                )
        except OperationalError:
            self.log.error("DataQualityCheckOperator failed.")
            raise Exception("DataQualityCheckOperator failed.")
        self.log.info('DataQualityCheckOperator Success!')

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
                "DataQualityCheckOperator failed. "
                "Cannot connect to PostgresSQL."
            )
        return _psql_conn
