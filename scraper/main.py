# scraper/main.py


import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from unece_ports.spiders.ports_spider import PortsSpider


settings_file_path = 'unece_ports.settings'
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
settings = get_project_settings()
process = CrawlerProcess(settings=settings)
process.crawl(PortsSpider)
process.start()
