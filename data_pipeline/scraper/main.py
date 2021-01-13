# data_pipeline/scraper/main.py


import sys
import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def crawl_unece_ports():
    sys.path.insert(0, '../airflow/scraper/unece_ports')
    from unece_ports.spiders.ports_spider import (
        PortsSpider
    )
    settings_file_path = 'unece_ports.settings'
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
    settings = get_project_settings()
    process = CrawlerProcess(settings=settings)
    process.crawl(PortsSpider)
    process.start()


if __name__ == "__main__":
    crawl_unece_ports()
