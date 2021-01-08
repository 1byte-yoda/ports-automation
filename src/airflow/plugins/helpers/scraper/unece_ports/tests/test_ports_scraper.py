# helpers/scraper/unece_ports/tests/test_ports_scraper.py


from re import match, Match
from typing import Generator, Union
from helpers.scraper.unece_ports.tests import (
    fake_response_from_file
)
from helpers.scraper.unece_ports.tests.base import (
    BaseTest
)


class PortsSpiderTest(BaseTest):

    def test_parse_horizontal_scraper(self):
        """
        Test if horizontal scraper parse() returns Request w.r.t. country.
        """
        url = (
            'https://unece.org/cefact/'
            'unlocode-code-list-country-and-territory'
        )
        results = self.spider.parse(
            fake_response_from_file(
                file_name='response/countries/sample.html',
                url=url
            )
        )
        expected_request_count = 249
        for idx, item in enumerate(results):
            self.assertEqual('GET', item.method)
            self.assertIn('country_name', item.cb_kwargs)
            self.assertIsNotNone(item.cb_kwargs['country_name'])
            url_match = self._unlocode_url_matcher(item.url)
            self.assertIsNotNone(url_match)
        total_length = idx + 1
        self.assertEqual(total_length, expected_request_count)

    def test_parse_ports_pandas(self):
        """
        Test if parse_ports_pandas returns the expected shape of the data.
        """
        results = self.spider.parse_port_pandas(
            fake_response_from_file('response/ports/sample.html')
        )
        self._test_item_results(results, 12)

    def test_parse_ports_pandas_no_table_found_handles_correctly(self):
        """
        Test pandas scraper handles error related to changes on the website.
        """
        results = self.spider.parse_port_pandas(
            fake_response_from_file(
                file_name='response/ports/no_table_elem.html',
            )
        )
        for item in results:
            self.assertIn('portName', item)
            self.assertIn('coordinates', item)
            self.assertIn('unlocode', item)
            self.assertIn('countryName', item)
            self.assertEqual('no_available_port', item['coordinates'])
            self.assertEqual('no_available_port', item['unlocode'])
            self.assertEqual('no_available_port', item['portName'])

    def test_parse_ports_xpath(self):
        """
        Test if parse_ports_xpath returns the expected shape of the data.
        """
        results = self.spider.parse_port_xpath(
            fake_response_from_file('response/ports/sample.html')
        )
        self._test_item_results(results, 12)

    def test_parse_ports_xpath_no_table_found_handles_correctly(self):
        """
        Test if xpath scraper handles error related to changes on the website.
        """
        results = self.spider.parse_port_xpath(
            fake_response_from_file(
                file_name='response/ports/no_table_elem.html',
            )
        )
        for item in results:
            self.assertIn('portName', item)
            self.assertIn('coordinates', item)
            self.assertIn('unlocode', item)
            self.assertIn('countryName', item)
            self.assertEqual('no_available_port', item['coordinates'])
            self.assertEqual('no_available_port', item['unlocode'])
            self.assertEqual('no_available_port', item['portName'])

    def _test_item_results(self, results: Generator, expected_length: int):
        """Helper function that loops over spider items.

        :param Generator results:
            yielded spider items from scraper parse method.
        :param int expected_length:
            the expected number of items.
        """
        count = 0
        for item in results:
            count += 1
            self.assertEqual(
                ' '.join(item['countryName'].split()),
                '(GT) GUATEMALA'
            )
            self.assertIsNotNone(item['portName'])
            self.assertIsNotNone(item['unlocode'])
            self.assertIsNotNone(item['coordinates'])
        self.assertEqual(count, expected_length)

    def _unlocode_url_matcher(self, url: str) -> Union[Match, None]:
        """Function that identifies if a url matched a pattern.

        Sample match:
            https://service.unece.org/trade/locode/af.htm
        :param str url:
            the query url to test against the pattern
        :return Union[Match, None] object:
            an object of type Match if the url matched,
            None otherwise
        """
        pattern = (
            r'https://service.unece.org/trade/locode/'
            r'[a-z]{2}\.htm'
        )
        return match(pattern, url)
