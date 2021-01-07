# helpers/scraper/unece_ports/tests/base.py


from unittest import TestCase
from mongomock import MongoClient
from scrapy.utils.test import get_crawler
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
        _mongodb_uri = 'mongo://mock:mockpassword@mockhost:27017'
        config = [_mongodb_uri, 'unece_staging', 'ports']
        self._crawler = get_crawler(ports_spider.PortsSpider)
        self._crawler.settings = {
            'MONGO_DB_URI': config[0],
            'STAGING_PORTS_DB': config[1],
            'PORTS_TABLE': config[2]
        }
        self._mock_spider = self._crawler._create_spider(
            **self._get_spiderargs()
        )
        self._client = MongoClient()
        self._db = self._client.unece_staging
        self.pipeline = MongodbPipeline.from_crawler(self._crawler)
        self.pipeline.collection = self._db.ports

    def tearDown(self):
        self.pipeline.close_spider(self._mock_spider)
        self._client.close()

    def _get_spiderargs(self):
        allowed_domains = ['scrapytest.org']
        return dict(name='foo', allowed_domains=allowed_domains)
