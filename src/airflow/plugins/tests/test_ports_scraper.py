# helpers/scraper/unece_ports/tests/test_ports_scraper.py

import os
from unittest import TestCase
from re import match, Match
from typing import Generator, Union
from scrapy.http import HtmlResponse, Request
from helpers.scraper.unece_ports.spiders import (
    ports_spider
)


class BaseTest(TestCase):
    def setUp(self):
        self.spider = ports_spider.PortsSpider()

    @staticmethod
    def fake_response_from_file(file_name, url=None) -> HtmlResponse:
        """
        Create a Scrapy fake HTTP response from a HTML file
        :param str file_name:
            The relative filename from the responses directory,
            but absolute paths are also accepted.
        :param str url:
            The URL of the response.
        :returns:
            A scrapy HTTP response which can be used for unittesting.
        """
        if not url:
            url = 'https://service.unece.org/trade/locode/gt.htm'
        request = Request(url=url)
        if not file_name[0] == '/':
            responses_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(responses_dir, file_name)
        else:
            file_path = file_name
        with open(file_path, 'r') as f:
            file_content = f.read()
        response = HtmlResponse(
            url=url,
            request=request,
            body=file_content,
            encoding='utf-8'
        )
        return response


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
            BaseTest.fake_response_from_file(
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
            BaseTest.fake_response_from_file('response/ports/sample.html')
        )
        self._test_item_results(results, 12)

    def test_parse_ports_pandas_no_table_found_handles_correctly(self):
        """
        Test pandas scraper handles error related to changes on the website.
        """
        results = self.spider.parse_port_pandas(
            BaseTest.fake_response_from_file(
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
            BaseTest.fake_response_from_file('response/ports/sample.html')
        )
        self._test_item_results(results, 12)

    def test_parse_ports_xpath_no_table_found_handles_correctly(self):
        """
        Test if xpath scraper handles error related to changes on the website.
        """
        results = self.spider.parse_port_xpath(
            BaseTest.fake_response_from_file(
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
