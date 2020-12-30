# scraper/unece_ports/tests/test_ports.py


from unece_ports.tests import fake_response_from_file
from unece_ports.tests.base import BaseTest


class OsdirSpiderTest(BaseTest):
    def _test_item_results(self, results, expected_length):
        count = 0
        for item in results:
            count += 1
            self.assertIsNotNone(item['countryName'], 'GUATEMALA')
            self.assertIsNotNone(item['portName'])
            self.assertIsNotNone(item['unlocode'])
            self.assertIsNotNone(item['coordinates'])
        self.assertEqual(count, expected_length)

    def test_parse(self):
        results = self.spider.parse_port(
            fake_response_from_file('response/ports/sample.html')
        )
        self._test_item_results(results, 12)
