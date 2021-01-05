# helpers/scraper/unece_ports/settings.py


import os
from datetime import datetime
from dotenv import load_dotenv


# Setup housekeeping values
base_dir = os.path.abspath(
    os.path.dirname(os.path.dirname(__file__))
)
env_file = os.path.join(base_dir, '.env')
load_dotenv()
current_datetime = datetime.strftime(
    datetime.now(),
    'Date_%Y_%m_%d_Time_%T'
).replace(':', '_')
logs_filename = f'logs_({current_datetime}).txt'
logs_folder_dir = os.path.join(base_dir, 'tmp')
logs_file_dir = os.path.join(logs_folder_dir, logs_filename)


BOT_NAME = 'unece_ports'


SPIDER_MODULES = ['helpers.scraper.unece_ports.spiders']
NEWSPIDER_MODULE = 'helpers.scraper.unece_ports.spiders'


# Obey robots.txt rules
ROBOTSTXT_OBEY = True


# Pipelines
ITEM_PIPELINES = {
    'helpers.scraper.unece_ports.pipelines.mongodb.MongodbPipeline': 1
}


# Logging
LOG_LEVEL = 'INFO'


# Retry
RETRY_ENABLED = True
RETRY_TIMES = 10
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 400, 408, 429]


# Auto Throttle
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_DEBUG = True
DOWNLOAD_DELAY = 0.25


# PostgreSQL
POSTGRESQL_PIPELINE_URL = (
    os.environ.get('POSTGRESQL_URL')
)


# MongoDB
MONGO_DB_URI = os.environ.get('MONGO_DB_URI')
STAGING_PORTS_DB = os.environ.get('STAGING_PORTS_DB')
PORTS_TABLE = os.environ.get('PORTS_TABLE')


# CLOSESPIDER_ITEMCOUNT = 1
