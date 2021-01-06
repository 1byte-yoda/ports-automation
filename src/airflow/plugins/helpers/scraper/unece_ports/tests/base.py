# helpers/scraper/unece_ports/tests/base.py


from unittest import TestCase
from mongomock import MongoClient
from helpers.scraper.unece_ports.spiders import (
    ports_spider
)
from helpers.scraper.unece_ports.pipelines.mongodb import (
    MongodbPipeline
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


class MongodbPipelineBaseTest(TestCase):
    def setUp(self):
        self._client = MongoClient()
        self._db = self._client.unece_staging
        _mongodb_uri = 'mongodb://mock:mockpassword@mockhost:27017'
        config = [_mongodb_uri, 'unece_staging', 'ports']
        self.pipeline = MongodbPipeline(config=config)
        self.pipeline.collection = self._db.ports
        self._mock_spider = ports_spider.PortsSpider()

    def tearDown(self):
        self.pipeline.close_spider(self._mock_spider)
        self._client.close()
