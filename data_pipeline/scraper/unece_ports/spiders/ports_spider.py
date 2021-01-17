# data_pipeline/scraper/unece_ports/spiders/ports_spider.py


from urllib.parse import urljoin
from scrapy import Spider
from scrapy.http import Request
from unece_ports.items import (
    UnecePortsItem
)
from unece_ports.item_loaders import (
    PortItemLoader
)
from unece_ports.spiders.lib.utils import (
    get_data
)


_NO_AVAILABLE_PORT = 'no_available_port'
DEFAULT_ITEM = {
    'portName': _NO_AVAILABLE_PORT,
    'coordinates': _NO_AVAILABLE_PORT,
    'unlocode': _NO_AVAILABLE_PORT
}


class PortsSpider(Spider):
    name = 'ports_spider'
    allowed_domains = ['unece.org']
    start_urls = [
        'https://unece.org/cefact/unlocode-code-list-country-and-territory'
    ]

    def parse(self, response):
        """This function parses url links of countries (horizontal parsing),
        then yield the url into a callback function for vertical parsing.
        """
        # assuming that every country column has 1 child
        country_selector = response.xpath(
            '//*[contains(@class, "content_30739 page_23770")]'
            '//table//tr//a[1]'
        )
        for selector in country_selector:
            url = selector.xpath('./@href').extract_first()
            country_name = selector.xpath('./text()').extract_first()
            current_country_url = urljoin(response.url, url)
            yield Request(
                url=current_country_url,
                callback=self.parse_port_pandas,
                cb_kwargs={'country_name': country_name}
            )

    def parse_start_url_test(self, response):
        """This function parses url links of countries (horizontal parsing),
        then yield the url into a callback function for vertical parsing.
        This test will ensure that href tags are available in the table.
        @url https://unece.org/cefact/unlocode-code-list-country-and-territory
        @returns items 1
        @scrapes country_href
        """
        # assuming that every country column has 1 child
        country_selector = response.xpath(
            '//*[contains(@class, "content_30739 page_23770")]'
            '//table//tr//a[1]/@href'
        ).extract()
        for selector in country_selector:
            yield {'country_href': selector}

    def parse_port_pandas(self, response, country_name="country"):
        """This function uses pandas to parse UN/LOCODE per country.
        Faster implementation than parse_port_xpath.
        @url https://service.unece.org/trade/locode/gt.htm
        @returns items 1
        @scrapes countryName portName coordinates unlocode
        """
        ports = get_data(
            response=response
        )
        if ports and ports.iter:
            ports_iterator = ports.iter
            for _, row in ports_iterator:
                port_item = UnecePortsItem()
                port_item_loader = PortItemLoader(item=port_item)
                name, locode, coordinate = row.values
                port_item_loader.add_value(
                    field_name='countryName', value=ports.countryName
                )
                port_item_loader.add_value(
                    field_name='portName', value=name
                )
                port_item_loader.add_value(
                    field_name='unlocode', value=locode
                )
                port_item_loader.add_value(
                    field_name='coordinates', value=coordinate
                )
                yield port_item_loader.load_item()
        else:
            DEFAULT_ITEM['countryName'] = country_name
            yield DEFAULT_ITEM

    def parse_port_xpath(self, response):
        """This function uses xpath to parse UN/LOCODE per country.
        Same as what parse_port_pandas scrapes.
        @url https://service.unece.org/trade/locode/gt.htm
        @returns items 1
        @scrapes countryName portName coordinates unlocode
        """
        position = (
            'count(//td[a[contains(@href, "{0}")]]'
            '//preceding-sibling::*)+1'
        )
        raw_table = response.xpath(
            '//table[position()=3]'
            f'//tr[td[position() = {position.format("#Function")}]'
            '[contains(text(), "1")]]'
        )
        country_name = response.xpath(
            '//table[position()=2]//td//text()'
        ).extract_first()
        if raw_table:
            names_port = raw_table.xpath(
                './/td[position() = '
                f'{position.format("#NameWoDiacritics")}]/text()'
            ).extract()
            locodes_port = raw_table.xpath(
                './/td[position() = '
                f'{position.format("#LOCODE")}]/text()'
            ).extract()
            coordinates_port = raw_table.xpath(
                './/td[position() = '
                f'{position.format("#Coordinates")}]/text()'
            ).extract()
            port_zipped_data = zip(
                names_port,
                locodes_port,
                coordinates_port
            )
            for (name, locode, coord) in port_zipped_data:
                port_item = UnecePortsItem()
                port_item_loader = PortItemLoader(item=port_item)
                port_item_loader.add_value(
                    field_name='countryName', value=country_name
                )
                port_item_loader.add_value(
                    field_name='portName', value=name
                )
                port_item_loader.add_value(
                    field_name='unlocode', value=locode
                )
                port_item_loader.add_value(
                    field_name='coordinates', value=coord
                )
                yield port_item_loader.load_item()
        else:
            DEFAULT_ITEM['countryName'] = country_name
            yield DEFAULT_ITEM
