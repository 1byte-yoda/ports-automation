# scraper/unece_ports/pipelines/postgresql.py


import json
import traceback
import dj_database_url
import psycopg2
import pika
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

        # Get RabbitMQ settings
        rq_host = crawler.settings.get("RABBITMQ_HOST")
        rq_port = crawler.settings.get("RABBITMQ_PORT")
        rq_user = crawler.settings.get("RABBITMQ_USER")
        rq_password = crawler.settings.get("RABBITMQ_PASSWORD")
        rq_virtual_host = crawler.settings.get("RABBITMQ_VIRTUAL_HOST")
        rq_exchange = crawler.settings.get("RABBITMQ_EXCHANGE")
        rq_routing_key = crawler.settings.get("RABBITMQ_ROUTING_KEY")
        rq_queue = crawler.settings.get("RABBITMQ_QUEUE")

        # Get PostgreSQL URL from settings
        postgresql_url = crawler.settings.get('POSTGRESQL_PIPELINE_URL', None)

        # If doesn't exist, disable the pipeline
        if not postgresql_url:
            raise NotConfigured

        # Create the class
        return cls(
            postgresql_url,
            rq_host,
            rq_port,
            rq_user,
            rq_password,
            rq_virtual_host,
            rq_exchange,
            rq_routing_key,
            rq_queue
        )

    def __init__(
        self,
        postgresql_url,
        rq_host,
        rq_port,
        rq_user,
        rq_password,
        rq_virtual_host,
        rq_exchange,
        rq_routing_key,
        rq_queue
    ):
        """Opens a PostgreSQL and a RabbitMQ connection pool"""

        # Store the url for future reference
        self.postgresql_url = postgresql_url
        # Report connection error only once
        self.report_connection_error = True

        # Parse PostgreSQL URL and try to initialize a connection
        conn_kwargs = PostgresqlWriter.parse_postgresql_url(postgresql_url)

        self.rq_host = rq_host
        self.rq_port = rq_port
        self.rq_user = rq_user
        self.rq_password = rq_password
        self.rq_virtual_host = rq_virtual_host
        credentials = pika.credentials.PlainCredentials(
            self.rq_user, self.rq_password
        )
        parameters = pika.ConnectionParameters(
            self.rq_host,
            self.rq_port,
            self.rq_virtual_host,
            credentials
        )
        self.rq_connection = pika.BlockingConnection(parameters=parameters)
        self.rq_channel = self.rq_connection.channel()
        self.rq_exchange = rq_exchange
        self.rq_routing_key = rq_routing_key
        self.rq_queue = rq_queue
        self.rq_channel.exchange_declare(
            exchange=rq_exchange,
            exchange_type="direct",
            durable=True
        )
        self.queue_state = self.rq_channel.queue_declare(
            queue=rq_queue,
            durable=True
        )
        self.rq_channel.queue_bind(
            exchange=rq_exchange,
            routing_key=rq_routing_key,
            queue=rq_queue
        )
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

        queue_empty = self.queue_state.method.message_count == 0
        if not queue_empty:
            method, properties, body = self.rq_channel.basic_get(
                self.rq_queue
            )
            body = json.loads(body)
            try:
                yield self.dbpool.runInteraction(self.do_insert, body)
            except psycopg2.OperationalError:
                if self.report_connection_error:
                    logger.error("Can't connect to PostgreSQL: %s" %
                                self.postgresql_url)
                    self.report_connection_error = False
            except Exception:
                print(traceback.format_exc())

            # Return the item for the next stage
            defer.returnValue(body)

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
