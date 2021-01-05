# helpers/scraper/unece_ports/item_loaders/__init__.py


from itemloaders.processors import (
    TakeFirst,
    MapCompose
)
from scrapy.loader import ItemLoader
from helpers.scraper.unece_ports.item_loaders.processors import (
    process_country_name,
    process_coordinates,
    process_port_name,
    process_unlocodo
)


class PortItemLoader(ItemLoader):
    default_output_processor = TakeFirst()
    countryName_in = MapCompose(process_country_name)
    coordinates_in = MapCompose(process_coordinates)
    portName_in = MapCompose(process_port_name)
    unlocode_in = MapCompose(process_unlocodo)
