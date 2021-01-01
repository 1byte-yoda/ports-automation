# scraper/unece_ports/settings.py


import os
from datetime import datetime
from dotenv import load_dotenv



base_dir = os.path.abspath(
    os.path.dirname(os.path.dirname(__file__))
)
env_file = os.path.join(base_dir, '.env.dev')
load_dotenv()
current_datetime = datetime.strftime(
    datetime.now(),
    'Date_%Y_%m_%d_Time_%T'
).replace(':', '_')
logs_filename = f'logs_({current_datetime}).txt'
logs_folder_dir = os.path.join(base_dir, 'tmp')
logs_file_dir = os.path.join(logs_folder_dir, logs_filename)


BOT_NAME = 'unece_ports'


SPIDER_MODULES = ['unece_ports.spiders']
NEWSPIDER_MODULE = 'unece_ports.spiders'


# Obey robots.txt rules
ROBOTSTXT_OBEY = True


# Pipelines
ITEM_PIPELINES = {
    # 'unece_ports.pipelines.kafka.KafkaWorker': 200,
    'scrapy_kafka_export.KafkaItemExporterExtension': 1
    # 'unece_ports.pipelines.postgresql.PostgresqlWriter': 2
    # 'unece_ports.pipelines.redis.RedisPublisherPipeline': 1,
}


# Logging
# LOG_STDOUT = True
# LOG_FILE = logs_file_dir
LOG_LEVEL = 'INFO'


# Retry
RETRY_ENABLED = True
RETRY_TIMES = 4
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 400, 408, 429]


# Auto Throttle
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 3
AUTOTHROTTLE_DEBUG = True
DOWNLOAD_DELAY = 0.25


# PostgreSQL
POSTGRESQL_PIPELINE_URL = (
    os.environ.get('POSTGRESQL_URL')
)


# TODO: Store access data to Env file
# RabbitMQ
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'guest'
RABBITMQ_PASSWORD = 'guest'
RABBITMQ_VIRTUAL_HOST = '/'
RABBITMQ_EXCHANGE = 'scrapy'
RABBITMQ_ROUTING_KEY = 'item'
RABBITMQ_QUEUE = 'item'
RABBITMQ_URI = 'amqp://userid:mypass%4025@myrabbitserver:5672/filestream'


# MongoDB
MONGO_DB_URI = 'mongodb://root:rootpassword@localhost:27017/tracking?authSource=admin'
MONGO_DATABASE = 'unece_dev'
MONGO_PORTS_TABLE = 'ports'


# Kafka
KAFKA_EXPORT_ENABLED = True
KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS').split(',')
# docker exec -it 644db4429cfb kafka-topics --zookeeper zookeeper:2181 --create --topic ports-topic --partitions 1 --replication-factor 3
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC')
KAFKA_BATCH_SIZE = 100
