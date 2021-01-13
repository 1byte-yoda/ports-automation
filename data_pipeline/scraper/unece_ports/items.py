# data_pipeline/scraper/unece_ports/items.py


from scrapy import Field, Item


class UnecePortsItem(Item):
    # Primary fields
    portName = Field()
    unlocode = Field()
    countryName = Field()
    coordinates = Field()
