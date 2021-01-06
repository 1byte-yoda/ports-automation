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

    def test_item_loader_processor_process_coordinates(self):
        """
        Test if process_coordinates returns the expected coordinates format.
        """
        coordinates1 = self.process_coordinates('\n1418N 09155W\xa0')
        coordinates2 = self.process_coordinates('1550N 08845W\xa0\xa0\t')
        missing_value = self.process_coordinates('\xa0\xa0\t')
        self.assertEqual(coordinates1, '1418N 09155W')
        self.assertEqual(coordinates2, '1550N 08845W')
        self.assertEqual(missing_value, 'missing_value')

    def test_item_loader_processor_process_country_name(self):
        """
        Test if process_country_name returns the expected country name format.
        """
        country1 = self.process_country_name(
            '\t(GT)\xa0\xa0GUATEMALA\xa0\xa0\xa0\xa0'
        )
        country2 = self.process_country_name(
            '(GT)\xa0\xa0GUATEMALA\xa0\xa0\xa0\xa0\t'
        )
        missing_value = self.process_country_name('\xa0\xa0\xa0\xa0\t')
        self.assertEqual(country1, 'Guatemala')
        self.assertEqual(country2, 'Guatemala')
        self.assertEqual(missing_value, 'missing_value')

    def test_item_loader_processor_process_port_name(self):
        """
        Test if process_port_name returns the expected port name format.
        """
        port_name1 = self.process_port_name('\tLa Aurora\xa0')
        port_name2 = self.process_port_name('Livingston \xa0\t')
        port_name3 = self.process_port_name('livingston \xa0\t')
        missing_value = self.process_port_name('\xa0\t')
        self.assertEqual(port_name1, 'La Aurora')
        self.assertEqual(port_name2, 'Livingston')
        self.assertEqual(port_name3, 'Livingston')
        self.assertEqual(missing_value, 'missing_value')

    def test_item_loader_processor_process_unlocode(self):
        """
        Test if process_port_name returns the expected unlocode format.
        """
        locode1 = self.process_unlocode('GT\xa0\xa0LAA\xa0\t')
        locode2 = self.process_unlocode('\tGT\xa0LIV\xa0')
        locode3 = self.process_unlocode('\tgt\xa0liV\xa0')
        missing_value = self.process_unlocode('\t\xa0')
        self.assertEqual(locode1, 'GT LAA')
        self.assertEqual(locode2, 'GT LIV')
        self.assertEqual(locode3, 'GT LIV')
        self.assertEqual(missing_value, 'missing_value')
