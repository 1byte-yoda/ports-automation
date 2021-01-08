import json
import traceback
from psycopg2.errors import OperationalError
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class DataQualityCheckOperator(BaseOperator):
    """
    Airflow operator that runs SQL queries to check the quality
    of downstream data.
    """
    ui_color = '#28df99'

    @apply_defaults
    def __init__(self, tables, postgres_config, queries, *args, **kwargs):
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
        self._queries = queries
        self._postgres_conn_id = postgres_config.conn_id

    def execute(self, context):
        """Does data quality checks for each table in table list.
        Assert a list of tables against a business defined SQL metrics.
        """
        self.log.info('DataQualityCheckOperator Starting...')
        self.log.info("Initializing Postgres Master DB Connection...")
        psql_hook = PostgresHook(postgres_conn_id=self._postgres_conn_id)
        try:
            for table in self._tables:
                data_quality = dict()
                for name, query in self._queries.items():
                    self.log.info(f"Running query: {query}")
                    result = psql_hook.get_first(sql=query)
                    result = result.get('count')
                    if not result:
                        error = (
                            "Data quality check FALED. "
                            f"{table} returned no results "
                            f"for query: {name}"
                        )
                        self.log.error(error)
                        raise ValueError(error)
                    data_quality[name] = result
                self.log.info(
                    f"Data quality check on table '{table}' PASSED\n"
                    "Results Summary:\n"
                    f"{json.dumps(data_quality, indent=4, sort_keys=True)}"
                )
        except OperationalError:
            self.log.error("DataQualityCheckOperator FALED.")
            raise Exception("DataQualityCheckOperator FALED.")
        except Exception:
            self.log.error(traceback.format_exc())
            raise Exception("DataQualityCheckOperator FALED.")
        self.log.info('DataQualityCheckOperator SUCCESS!')
