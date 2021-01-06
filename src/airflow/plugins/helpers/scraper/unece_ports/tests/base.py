# helpers/scraper/unece_ports/tests/base.py


from unittest import TestCase
from helpers.scraper.unece_ports.spiders import (
    ports_spider
)
from helpers.scraper.unece_ports.item_loaders.processors import (
    process_country_name,
    process_coordinates,
    process_port_name,
    process_unlocode
)


class BaseTest(TestCase):
    def setUp(self):
        self.spider = ports_spider.PortsSpider()
        self.process_coordinates = process_coordinates
        self.process_country_name = process_country_name
        self.process_port_name = process_port_name
        self.process_unlocode = process_unlocode
