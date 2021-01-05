# helpers/scraper/unece_ports/tests/base.py


from unittest import TestCase
from helpers.scraper.unece_ports.spiders import (
    ports_spider
)


class BaseTest(TestCase):
    def setUp(self):
        self.spider = ports_spider.PortsSpider()
