from unittest import TestCase
from unece_ports.spiders import ports_spider


class BaseTest(TestCase):
    def setUp(self):
        self.spider = ports_spider.PortsSpider()
