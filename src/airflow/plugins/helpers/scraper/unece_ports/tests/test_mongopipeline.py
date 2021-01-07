# helpers/scraper/unece_ports/tests/test_mongopipeline.py


from twisted.internet import defer
from scrapy.exceptions import NotConfigured
from helpers.scraper.unece_ports.tests.base import (
    MongodbPipelineBaseTest
)
from helpers.scraper.unece_ports.pipelines.mongodb import (
    MongodbPipeline,
    TransactionError
)


class MongodbPipelineTest(MongodbPipelineBaseTest):

    @defer.inlineCallbacks
    def test_mongo_pipeline_pymongo_transaction_error(self):
        with self.assertRaises(TransactionError) as exc_info:
            yield self.pipeline.process_item(
                {'foo', 'bar'}, self._mock_spider
            )
        self.assertIn(
            'error occured during transaction',
            str(exc_info.exception)
        )

    @defer.inlineCallbacks
    def _test_process_item(self, total_items=10):
        item = dict()
        for idx in range(total_items):
            item['portName'] = f'port_{idx}'
            item['unlocode'] = f'unlocode_{idx}'
            item['countryName'] = f'countryName_{idx}'
            item['coordinates'] = f'coordinates_{idx}'
            yield self.pipeline.process_item(
                item, self._mock_spider
            )

    def test_process_item_single_call(self):
        """
        Test MongoDB pipeline to upsert data in single function call
        """
        expected_items = 1
        self._test_process_item(total_items=expected_items)
        collection = self.pipeline.collection
        total_inserted = collection.count_documents({})
        self.assertEqual(total_inserted, expected_items)

    def test_process_item_mutli_call(self):
        """
        Test MongoDB pipeline to upsert data in multi function call
        """
        expected_items = 10
        self._test_process_item(total_items=expected_items)
        collection = self.pipeline.collection
        total_inserted = collection.count_documents({})
        self.assertEqual(total_inserted, expected_items)

    def test_mongo_pipeline_not_configured(self):
        with self.assertRaises(NotConfigured) as exc_info:
            self._crawler.settings = {
                'MONGO_DB_URI': None,
                'STAGING_PORTS_DB': None,
                'PORTS_TABLE': None
            }
            MongodbPipeline.from_crawler(self._crawler)
        self.assertIn('not configured', str(exc_info.exception))
