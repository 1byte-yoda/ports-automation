# helpers/scraper/unece_ports/tests/test_mongopipeline.py


import sys
from unittest import TestCase
from twisted.internet import defer
from scrapy.utils.test import get_crawler
from scrapy.exceptions import NotConfigured
from mongomock import MongoClient


# Insert path to discover module imports
sys.path.insert(0, '../airflow/scraper')


class MongodbPipelineTest(TestCase):

    def setUp(self):
        _mongodb_uri = 'mongodb://mock:mockpassword@mockhost:27017'
        config = [_mongodb_uri, 'unece_staging', 'ports']
        from unece_ports.spiders import ports_spider
        self._crawler = get_crawler(ports_spider.PortsSpider)
        self._crawler.settings = {
            'MONGO_DB_URI': config[0],
            'STAGING_DB': config[1],
            'TABLE_PORTS': config[2]
        }
        self._mock_spider = self._crawler._create_spider(
            **self._get_spiderargs()
        )
        self._client = MongoClient()
        self._db = self._client.unece_staging
        from unece_ports.pipelines.mongodb import MongodbPipeline
        self.pipeline = MongodbPipeline.from_crawler(self._crawler)
        self.pipeline.collection = self._db.ports

    def tearDown(self):
        self.pipeline.close_spider(self._mock_spider)
        self._client.close()

    def test_mongo_pipeline_wrong_uri_handled(self):
        """Test if mongo pipeline can handle wrong database uri prefix."""
        wrong_uri = 'mongo://mock:mockpassword@mockhost:27017'
        config = [wrong_uri, 'unece_staging', 'ports']
        crawler = self._crawler
        crawler.settings = {
            'MONGO_DB_URI': config[0],
            'STAGING_DB': config[1],
            'TABLE_PORTS': config[2]
        }
        from unece_ports.pipelines.mongodb import MongodbPipeline
        pipeline = MongodbPipeline.from_crawler(crawler)
        self.assertEqual(
            'mongodb://mock:mockpassword@mockhost:27017',
            pipeline.mongo_url
        )

    @defer.inlineCallbacks
    def test_mongo_pipeline_pymongo_transaction_error_report(self):
        """Test if mongo pipeline throws an error when the parameter
        report_connection_error = True.
        """
        self.pipeline.report_connection_error = True
        from unece_ports.pipelines.mongodb import TransactionError
        with self.assertRaises(TransactionError) as exc_info:
            yield self.pipeline.process_item(
                {'foo', 'bar'}, self._mock_spider
            )
        self.assertIn(
            'error occured during transaction',
            str(exc_info.exception)
        )

    @defer.inlineCallbacks
    def test_mongo_pipeline_pymongo_transaction_error_not_report(self):
        """Test if mongo pipeline throws an error when the parameter
        report_connection_error = False.
        """
        self.pipeline.report_connection_error = False
        from unece_ports.pipelines.mongodb import TransactionError
        with self.assertRaises(TransactionError) as exc_info:
            yield self.pipeline.process_item(
                {'foo', 'bar'}, self._mock_spider
            )
        self.assertIn(
            'error occured during transaction',
            str(exc_info.exception)
        )

    def test_process_item_single_call(self):
        """
        Test mongo pipeline to upsert data in single function call
        """
        expected_items = 1
        self._test_process_item(total_items=expected_items)
        collection = self.pipeline.collection
        total_inserted = collection.count_documents({})
        self.assertEqual(total_inserted, expected_items)

    def test_process_item_mutli_call(self):
        """
        Test mongo pipeline to upsert data in multi function call
        """
        expected_items = 10
        self._test_process_item(total_items=expected_items)
        collection = self.pipeline.collection
        total_inserted = collection.count_documents({})
        self.assertEqual(total_inserted, expected_items)

    def test_mongo_pipeline_not_configured_error(self):
        """
        Test if mongo pipeline handles error when settings aren't configured.
        """
        from unece_ports.pipelines.mongodb import MongodbPipeline
        with self.assertRaises(NotConfigured) as exc_info:
            self._crawler.settings = {
                'MONGO_DB_URI': None,
                'STAGING_DB': None,
                'TABLE_PORTS': None
            }
            MongodbPipeline.from_crawler(self._crawler)
        self.assertIn('not configured', str(exc_info.exception))

    def _get_spiderargs(self):
        """Helper function that generates mock spider args."""
        allowed_domains = ['scrapytest.org']
        return dict(name='foo', allowed_domains=allowed_domains)

    @defer.inlineCallbacks
    def _test_process_item(self, total_items=10):
        """Helper function that executes pipeline.process_item."""
        item = dict()
        for idx in range(total_items):
            item['portName'] = f'port_{idx}'
            item['unlocode'] = f'unlocode_{idx}'
            item['countryName'] = f'countryName_{idx}'
            item['coordinates'] = f'coordinates_{idx}'
            yield self.pipeline.process_item(
                item, self._mock_spider
            )
