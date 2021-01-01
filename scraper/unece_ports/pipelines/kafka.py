import json
import time
from kafka import KafkaConsumer


class KafkaWorker(object):
    """
    Pipeline that writes to PostgreSQL databases
    """

    @classmethod
    def from_crawler(cls, crawler):
        """Retrieves scrapy crawler and accesses pipeline's settings"""


        # Get Kafka Config from settings
        brokers = crawler.settings.get('KAFKA_BROKERS', None)
        topic = crawler.settings.get('KAFKA_TOPIC', None)

        # If doesn't exist, disable the pipeline
        if not brokers or not topic:
            raise NotConfigured

        # Create the class
        return cls(brokers, topic)

    def __init__(self, brokers: list, topic: str):
        """Opens a PostgreSQL and a RabbitMQ connection pool"""
        self.consumer = KafkaConsumer(bootstrap_servers=brokers)
        self.consumer.subscribe(topic)

    def process_item(self, item, spider):
        while True:
            try: 
                message = self.consumer.poll(5.0)
                if not message:
                    time.sleep(5) # Sleep for 2 minutes
                if message.error():
                    print(f"Consumer error: {message.error()}")
                    continue
                buffer_data = json.loads(message)
                print(buffer_data)
            except Exception:
                pass

    def close_spider(self, spider):
        """Discard the consumer on spider close"""
        self.consumer.unsubscribe()
        self.consumer.close()