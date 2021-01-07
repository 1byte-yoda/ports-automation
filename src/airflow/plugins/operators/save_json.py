import traceback
from pathlib import Path
from jsonstreams import Stream, Type
from psycopg2.errors import OperationalError
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from helpers.lib.sql_queries import SqlQueries


class LoadToJsonOperator(BaseOperator):
    """
    Airflow operator for getting SQL query result
    and writting it into a json file.
    """
    ui_color = '#99f3bd'

    @apply_defaults
    def __init__(self, postgres_config, path, tables, *args, **kwargs):
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
        self._date_format = '%Y-%m-%d_%H-%M-%S'

    def execute(self, context):
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
            psql_cursor = psql_conn.cursor()
            execution_date = context.get('execution_date')
            if execution_date:
                execution_date = execution_date.format(self._date_format)
            select_all_query_to_json = SqlQueries.select_all_query_to_json
            for table in self._tables:
                ports_select_all = select_all_query_to_json.format(
                    table=table
                )
                psql_cursor.execute(ports_select_all)
                file_name = f"{table}_{execution_date}.json"
                self.save_to_json(psql_cursor, table, file_name)
        except OperationalError:
            self.log.error("LoadToJsonOperator failed.")
            raise Exception("LoadToJsonOperator failed. OperationalError")
        except OSError:
            self.log.error("Writting JSON file failed. Invalid file name.")
            raise OSError(
                "LoadToJsonOperator failed. "
                f"Invalid file name: {file_name}."
            )
        except Exception:
            self.log.error(traceback.format_exc())
            raise Exception("LoadToJsonOperator failed.")
        finally:
            self.log.info('Closing database connections...')
            psql_conn.close()
        self.log.info('LoadToJsonOperator Success!')

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
        except ValueError:
            self.log.error(
                "Writting JSON file failed. "
                "Invalid value found."
            )
            raise ValueError(
                "LoadToJsonOperator failed. "
                "Cannot write invalid value(s)."
            )
        self.log.info(f'JSON file exported successfuly on path: {file_name}.')
