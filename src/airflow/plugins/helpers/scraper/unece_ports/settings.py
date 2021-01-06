# helpers/scraper/unece_ports/settings.py


import os


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
RETRY_TIMES = 5
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 400, 408, 429]


# Auto Throttle
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 5
AUTOTHROTTLE_DEBUG = True
DOWNLOAD_DELAY = 0.25


# MongoDB
MONGO_DB_URI = os.environ.get('MONGO_DB_URI')
STAGING_PORTS_DB = os.environ.get('STAGING_PORTS_DB')
PORTS_TABLE = os.environ.get('PORTS_TABLE')


CLOSESPIDER_ITEMCOUNT = 1
