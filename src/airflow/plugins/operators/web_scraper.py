from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class WebScraperOperator(BaseOperator):
    """Airflow Operator that scrapes country unlocodes from
    https://unece.org/cefact/unlocode-code-list-country-and-territory

    Uses Scrapy module as the external module for scraping.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, web_scraper_func, *args, **kwargs):
        super().__init__(**kwargs)
        self._web_scraper_func = web_scraper_func

    def execute(self, context):
        self.log.info('WebScraperOperator Starting...')
        self._web_scraper_func()
        self.log.info('WebScraperOperator Successful!')
