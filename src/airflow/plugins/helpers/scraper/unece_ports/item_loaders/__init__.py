# helpers/scraper/unece_ports/item_loaders/__init__.py


from itemloaders.processors import TakeFirst
from scrapy.loader import ItemLoader


class PortItemLoader(ItemLoader):
    default_output_processor = TakeFirst()
