from urllib.parse import urljoin
from scrapy import Spider
from scrapy.http import Request
from unece_ports.items import UnecePortsItem
from unece_ports.item_loaders import PortItemLoader
from unece_ports.item_loaders.processors import DEFAULT_ITEM
from unece_ports.spiders.lib.utils import get_data


class PortsSpider(Spider):
    name = 'ports_spider'
    allowed_domains = ['unece.org']
    start_urls = [
        'https://unece.org/cefact/unlocode-code-list-country-and-territory'
    ]

    def parse(self, response):
        country_selector = response.xpath(
            '//*[contains(@class, "content_30739 page_23770")]'
            '//table//tr//a[1]/@href'
        ).extract()
        for url in country_selector:
            current_country_url = urljoin(response.url, url)
            yield Request(
                url=current_country_url,
                callback=self.parse_port_pandas
            )

    def parse_port_pandas(self, response):
        """This function parses UN/LOCODE per country.
        @url https://service.unece.org/trade/locode/gt.htm
        @returns items 1
        @scrapes countryName portName coordinates unlocode
        """
        ports = get_data(response_body=response.body)
        if ports:
            ports_iterator = ports.iter
            country_name = ports.country
            for _, row in ports_iterator:
                port_item = UnecePortsItem()
                port_item_loader = PortItemLoader(item=port_item)
                name, locode, coordinate = row.values
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
                    field_name='coordinates', value=coordinate
                )
                yield port_item_loader.load_item()
        else:
            DEFAULT_ITEM['countryName'] = country_name
            yield DEFAULT_ITEM

    def parse_port_xpath(self, response):
        """This function parses UN/LOCODE per country.
        @url https://service.unece.org/trade/locode/gt.htm
        @returns items 1
        @scrapes countryName portName coordinates unlocode
        """
        raw_table = response.xpath(
            '//table[position()=3]'
            '//tr[td[position()=6][contains(text(), "1")]]'
        )
        country_name = response.xpath(
            '//table[2]//tr//td//text()'
        ).extract_first()
        if raw_table:
            names_port = raw_table.xpath(
                './/td[position()=4]/text()'
            ).extract()
            locodes_port = raw_table.xpath(
                './/td[position()=2]/text()'
            ).extract()
            coordinates_port = raw_table.xpath(
                './/td[position()=10]/text()'
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
        DEFAULT_ITEM['countryName'] = country_name
        return DEFAULT_ITEM
