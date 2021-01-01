import json
import pymongo
import traceback
from kafka import KafkaConsumer
from scrapy.exceptions import NotConfigured
from twisted.internet import defer
from txmongo.connection import ConnectionPool
from unece_ports.pipelines.redis import RedisPublisherPipeline


class MongodbPipeline(object):
    """
    Pipeline that writes to Mongo Database
    """

    @classmethod
    def from_crawler(cls, crawler):
        """Retrieves scrapy crawler and accesses pipeline's settings"""

        # Get MongoDB URL from settings
        mongo_url = crawler.settings.get('MONGO_DB_URI', None)
        mongo_db = crawler.settings.get('MONGO_DATABASE', None)
        mongo_collection = crawler.settings.get('MONGO_PORTS_TABLE', None)

        # Get Kafka config from settings
        brokers = crawler.settings.get('KAFKA_BROKERS', None)
        topic = crawler.settings.get('KAFKA_TOPIC', None)

        config = [mongo_url, mongo_db, mongo_collection, brokers, topic]

        # If doesn't exist, disable the pipeline
        if not any(config):
            raise NotConfigured

        # Create the class
        return cls(config)

    def __init__(self, config):
        """Opens a MongoDB and a Kafka connection pool"""
        
        # Report connection error only once
        self.report_connection_error = True

        mongo_url, mongo_db, mongo_collection, brokers, topic = config

        self.consumer = KafkaConsumer(topic, bootstrap_servers=brokers)
        self.topic = topic

        # Setup MongoDB Connection
        self.mongo_url = mongo_url
        self.connection = ConnectionPool(mongo_url, connect_timeout=5)
        self.mongo_db = self.connection[mongo_db]
        self.collection = self.mongo_db[mongo_collection]

    def close_spider(self, spider):
        """Discard the database on spider close"""
        self.consumer.unsubscribe()
        self.consumer.close()
        self.connection.disconnect()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        """Processes the item. Does insert into MongoDB"""
        logger = spider.logger
        try:
            for buffer_data in self.consumer:
                buffer_data = json.loads(buffer_data)
                print(buffer_data)
                # yield self.connection.insert(buffer_data, safe=True)
        except pymongo.errors.OperationFailure:
            if self.report_connection_error:
                logger.error("Can't connect to MongoDB: %s" %
                             self.mongo_url)
                self.report_connection_error = False
        except Exception:
            print(traceback.format_exc())

        # Return the item for the next stage
        defer.returnValue(item)
