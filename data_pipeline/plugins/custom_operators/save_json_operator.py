import traceback
from pathlib import Path
from jsonstreams import Stream, Type
from psycopg2.extras import RealDictCursor
from psycopg2.errors import OperationalError, UndefinedTable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadToJsonOperator(BaseOperator):
    """
    Airflow operator for getting SQL query result
    and writting it into a json file.
    """
    ui_color = '#99f3bd'

    @apply_defaults
    def __init__(self, postgres_config, path, tables, query, *args, **kwargs):
        """
        Airflow operator for getting SQL query result
        and writting it into a json file.

        :param PostgresConfig postgres_config:
            instance of PostgresConfig class that provides connection
            parameters for Postgres connection.
        :param str path:
            save folder path of the expected json file.
        :param list tables:
            list of tables to be queried.
        """
        super(LoadToJsonOperator, self).__init__(*args, **kwargs)
        self._postgres_conn_id = postgres_config.conn_id
        self._path = Path(path)
        self._tables = tables
        self._query = query
        self._date_format = "%Y%m%dT%H%M%S"

    def execute(self, context, testing=False):
        """
        Query data from Postgresql master database and
        then write into a json file.

        The default json file name is the table name + the utc time.
        """
        self.log.info('LoadToJsonOperator Starting...')
        try:
            self.log.info("Initializing Postgres Master DB Connection...")
            psql_hook = PostgresHook(postgres_conn_id=self._postgres_conn_id)
            psql_conn = psql_hook.get_conn()
            psql_cursor = psql_conn.cursor(cursor_factory=RealDictCursor)
            execution_date = context.get('execution_date')
            execution_date = execution_date.strftime(self._date_format)
            for table in self._tables:
                ports_select_all = self._query.format(
                    table=table
                )
                psql_cursor.execute(ports_select_all)
                file_name = f"{table}_{execution_date}.json"
                self.save_to_json(psql_cursor, table, file_name)
        except (UndefinedTable, OperationalError):
            self.log.error("LoadToJsonOperator FAILED.")
            raise Exception("LoadToJsonOperator FAILED. OperationalError")
        except Exception:
            self.log.error(traceback.format_exc())
            raise Exception("LoadToJsonOperator FAILED.")
        finally:
            if not testing:
                self.log.info('Closing database connections...')
                psql_conn.close()
        self.log.info('LoadToJsonOperator SUCCESS!')

    def save_to_json(self, cursor, key, file_name):
        """Save SQL query results into a json file through streaming.

        :param generator cursor:
            generator that contains the query results and
            will be looped through when writting into a json file.
        :param str key:
            key that will be used for writting query results
        :param str file_name:
            file name of the expected json file.
        """
        if not self._path.exists():
            self.log.info(f'Path {self._path} does not exists.')
            self.log.info('Creating path...')
            self._path.mkdir(parents=True)
        file_name = self._path.joinpath(file_name)
        self.log.info(f'Writting query results to {file_name}.')
        try:
            with Stream(Type.object, filename=file_name) as stream:
                with stream.subarray(key) as subarray:
                    for row in cursor:
                        subarray.write(row)
        except Exception:
            self.log.error(
                "Writting JSON file FAILED. "
                "An error occured."
            )
            raise Exception(
                "LoadToJsonOperator FAILED."
            )
        self.log.info(f'JSON file exported SUCCESSFULLY on path: {file_name}.')
