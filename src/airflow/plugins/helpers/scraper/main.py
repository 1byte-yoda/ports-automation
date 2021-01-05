# helpers/scraper/main.py


import os
import dotenv
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from helpers.scraper.unece_ports.spiders.ports_spider import (
    PortsSpider
)


dotenv.load_dotenv()


def crawl_unece_ports():
    settings_file_path = 'helpers.scraper.unece_ports.settings'
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
    settings = get_project_settings()
    process = CrawlerProcess(settings=settings)
    process.crawl(PortsSpider)
    process.start()


if __name__ == "__main__":
    crawl_unece_ports()
