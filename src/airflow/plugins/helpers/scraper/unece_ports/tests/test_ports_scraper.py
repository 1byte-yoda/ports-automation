# helpers/scraper/unece_ports/tests/test_ports_scraper.py


from re import match, Match
from typing import Generator, Union
from helpers.scraper.unece_ports.tests import (
    fake_response_from_file
)
from helpers.scraper.unece_ports.tests.base import (
    BaseTest
)
import sys


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

    def test_parse_ports_pandas_no_table_found_raises_value_error(self):
        """
        Test if scraper handles error related to changes in website structure.
        """
        with self.assertRaises(ValueError) as exc_info:
            results = self.spider.parse_port_pandas(
                fake_response_from_file(
                    file_name='response/ports/no_table_elem.html',
                )
            )
            next(results)
        self.assertIn(
            'not enough values to unpack DataFrame',
            str(exc_info.exception)
        )

    def test_parse_ports_xpath(self):
        """
        Test if parse_ports_xpath returns the expected shape of the data.
        """
        results = self.spider.parse_port_xpath(
            fake_response_from_file('response/ports/sample.html')
        )
        self._test_item_results(results, 12)

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
            self.assertEqual(item['countryName'], 'Guatemala')
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
            'https://service.unece.org/trade/locode/'
            '[a-z]{2}\.htm'
        )
        return match(pattern, url)
