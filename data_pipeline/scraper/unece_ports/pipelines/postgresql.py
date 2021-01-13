# helpers/scraper/unece_ports/pipelines/postgresql.py


import traceback
import dj_database_url
import psycopg2
from twisted.internet import defer
from twisted.enterprise import adbapi
from scrapy.exceptions import NotConfigured


class PostgresqlWriter(object):
    """
    Pipeline that writes to PostgreSQL databases
    """

    @classmethod
    def from_crawler(cls, crawler):
        """Retrieves scrapy crawler and accesses pipeline's settings"""

        # Get PostgreSQL URL from settings
        postgresql_url = crawler.settings.get('POSTGRESQL_PIPELINE_URL', None)

        # If doesn't exist, disable the pipeline
        if not postgresql_url:
            raise NotConfigured

        # Create the class
        return cls(postgresql_url)

    def __init__(self, postgresql_url):
        """Opens a PostgreSQL and a RabbitMQ connection pool"""

        # Store the url for future reference
        self.postgresql_url = postgresql_url
        # Report connection error only once
        self.report_connection_error = True

        # Parse PostgreSQL URL and try to initialize a connection
        conn_kwargs = PostgresqlWriter.parse_postgresql_url(postgresql_url)
        self.dbpool = adbapi.ConnectionPool('psycopg2',
                                            connect_timeout=5,
                                            **conn_kwargs)

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.dbpool.close()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        """Processes the item. Does insert into PostgreSQL"""
        logger = spider.logger

        try:
            yield self.dbpool.runInteraction(self.do_insert, item)
        except psycopg2.OperationalError:
            if self.report_connection_error:
                logger.error("Can't connect to PostgreSQL: %s" %
                             self.postgresql_url)
                self.report_connection_error = False
        except Exception:
            print(traceback.format_exc())

        # Return the item for the next stage
        defer.returnValue(item)

    @staticmethod
    def do_insert(tx, item):
        """Does the actual INSERT INTO"""
        sql = """INSERT INTO ports (portName, unlocode, countryName, coordinates)
        VALUES (%s,%s,%s,%s);"""

        args = (
            item["portName"],
            item["unlocode"],
            item["countryName"],
            item["coordinates"]
        )
        tx.execute(sql, args)

    @staticmethod
    def parse_postgresql_url(postgresql_url):
        """
        Parses PostgreSQL url and prepares arguments for
        adbapi.ConnectionPool()
        """

        params = dj_database_url.parse(postgresql_url)

        conn_kwargs = {}
        conn_kwargs['host'] = params['HOST']
        conn_kwargs['user'] = params['USER']
        conn_kwargs['password'] = params['PASSWORD']
        conn_kwargs['dbname'] = params['NAME']
        conn_kwargs['port'] = params['PORT']

        # Remove items with empty values
        conn_kwargs = dict((k, v) for k, v in conn_kwargs.items() if v)

        return conn_kwargs
