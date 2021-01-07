import traceback
from psycopg2.errors import OperationalError
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from helpers.lib.sql_queries import SqlQueries


class DataQualityCheckOperator(BaseOperator):
    """
    Airflow operator that runs SQL queries to check the quality
    of downstream data.
    """
    ui_color = '#28df99'

    @apply_defaults
    def __init__(self, tables, postgres_config, *args, **kwargs):
        """
        Airflow operator that runs SQL queries to check the quality
        of downstream data.

        :param list tables:
            list of tables that will be checked against a SQL query.
        :param PostgresConfig postgres_config:
            instance of PostgresConfig class that provides connection
            parameters for Postgres connection.
        """
        super().__init__(**kwargs)
        self._tables = tables
        self._postgres_conn_id = postgres_config.conn_id
        self._datetime_format = "%m-%d-%Y %H:%M:%S"

    def execute(self, context):
        """Does data quality checks for each table in table list.
        Assert a list of tables against a business defined SQL metrics.
        """
        self.log.info('DataQualityCheckOperator Starting...')
        self.log.info("Initializing Postgres Master DB Connection...")
        psql_hook = PostgresHook(postgres_conn_id=self._postgres_conn_id)
        psql_conn = psql_hook.get_conn()
        psql_cursor = psql_conn.cursor()
        table_row_count = SqlQueries.table_row_count
        table_updated_count = SqlQueries.table_updated_count
        try:
            for table in self._tables:
                table_data_count = table_row_count.format(
                    table=table
                )
                table_updated_data_count = table_updated_count.format(
                    table=table,
                    execution_date=context.get(
                        'execution_date'
                    ).strftime(self._datetime_format)
                )
                psql_cursor.execute(table_data_count)
                result = psql_cursor.fetchone()
                data_count = result.get('count')
                psql_cursor.execute(table_updated_data_count)
                result = psql_cursor.fetchone()
                updated_data_count = result.get('total_updates')
                if not any([data_count, updated_data_count]):
                    error = (
                        "Data quality check failed. "
                        f"{table} returned no results."
                    )
                    self.log.error(error)
                    raise ValueError(error)
                self.log.info(
                    f"Data quality on table {table} check passed "
                    f"with {data_count} total records "
                    f"and {updated_data_count} total updated records."
                )
        except OperationalError:
            self.log.error("DataQualityCheckOperator failed.")
            raise Exception("DataQualityCheckOperator failed.")
        except Exception:
            self.log.error(traceback.format_exc())
            raise Exception("DataQualityCheckOperator failed.")
        finally:
            self.log.info('Closing database connections...')
            psql_conn.close()
        self.log.info('DataQualityCheckOperator Success!')
