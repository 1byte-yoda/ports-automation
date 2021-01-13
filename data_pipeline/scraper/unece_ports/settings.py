# helpers/scraper/unece_ports/settings.py


import os
import json
from dotenv import load_dotenv


load_dotenv()


BOT_NAME = 'unece_ports'


SPIDER_MODULES = ['unece_ports.spiders']
NEWSPIDER_MODULE = 'unece_ports.spiders'


# Obey robots.txt rules
ROBOTSTXT_OBEY = True


# Pipelines
ITEM_PIPELINES = {
    'unece_ports.pipelines.mongodb.MongodbPipeline': 1
}


# Logging
LOG_LEVEL = 'INFO'


# Retry
RETRY_ENABLED = True
RETRY_TIMES = 5
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 400, 408, 429]


# Auto Throttle
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 5
AUTOTHROTTLE_DEBUG = True
DOWNLOAD_DELAY = 0.25


# MongoDB
_user = os.environ.get('MONGO_INITDB_ROOT_USERNAME')
_password = os.environ.get('MONGO_INITDB_ROOT_PASSWORD')
_host = os.environ.get('MONGO_HOST')
_port = os.environ.get('MONGO_PORT')
_extras = json.loads(os.environ.get('MONGO_EXTRAS'))
_extras = '?'.join(f'{k}={v}' for k, v in _extras.items())
_staging_db = os.environ.get('STAGING_DB')
MONGO_DB_URI = (
    f'mongodb://{_user}:{_password}@{_host}:{_port}/{_staging_db}?{_extras}'
)
STAGING_DB = os.environ.get('STAGING_DB')
TABLE_PORTS = os.environ.get('TABLE_PORTS')


# Uncomment when in development
# CLOSESPIDER_ITEMCOUNT = 1
