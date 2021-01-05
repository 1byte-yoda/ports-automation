# helpers/scraper/unece_ports/tests/test_ports.py


from helpers.scraper.unece_ports.tests import (
    fake_response_from_file
)
from helpers.scraper.unece_ports.tests.base import (
    BaseTest
)


class PortsSpiderTest(BaseTest):
    def _test_item_results(self, results, expected_length):
        count = 0
        for item in results:
            count += 1
            self.assertEqual(item['countryName'], 'Guatemala')
            self.assertIsNotNone(item['portName'])
            self.assertIsNotNone(item['unlocode'])
            self.assertIsNotNone(item['coordinates'])
        self.assertEqual(count, expected_length)

    def test_parse_ports_pandas(self):
        """
        Test if parse_ports_pandas returns the expected shape of the data.
        """
        results = self.spider.parse_port_pandas(
            fake_response_from_file('response/ports/sample.html')
        )
        self._test_item_results(results, 12)

    def test_parse_ports_xpath(self):
        """
        Test if parse_ports_xpath returns the expected shape of the data.
        """
        results = self.spider.parse_port_xpath(
            fake_response_from_file('response/ports/sample.html')
        )
        self._test_item_results(results, 12)
