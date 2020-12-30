# scraper/unece_ports/settings.py


import os
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()
base_dir = os.path.abspath(
    os.path.dirname(os.path.dirname(__file__))
)
current_datetime = datetime.strftime(
    datetime.now(),
    'Date_%Y_%m_%d_Time_%T'
).replace(':', '_')
logs_filename = f'logs_({current_datetime}).txt'
logs_folder_dir = os.path.join(base_dir, "tmp")
logs_file_dir = os.path.join(logs_folder_dir, logs_filename)


BOT_NAME = "unece_ports"


SPIDER_MODULES = ["unece_ports.spiders"]
NEWSPIDER_MODULE = "unece_ports.spiders"


# Obey robots.txt rules
ROBOTSTXT_OBEY = True


# Pipelines
ITEM_PIPELINES = {
    'unece_ports.pipelines.postgresql.PostgresqlWriter': 700
}


# Logging
LOG_STDOUT = True
LOG_FILE = logs_file_dir
LOG_LEVEL = 'DEBUG'


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
    os.environ.get('POSTGRESQL_URL') or
    'postgresql://postgres:password@localhost/unece_dev'
)
