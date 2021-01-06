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
        """Airflow Operator that scrapes country unlocodes from
        https://unece.org/cefact/unlocode-code-list-country-and-territory

        Uses Scrapy module as the external module for scraping.

        :param Callable web_scraper_func:
            callble function that will run the scrapper.
        """
        super().__init__(**kwargs)
        self._web_scraper_func = web_scraper_func

    def execute(self, context):
        """Runs a web scraper function."""
        self.log.info('WebScraperOperator Starting...')
        try:
            self._web_scraper_func()
        except Exception:
            self.log.error('WebScraperOperator Failed.')
        self.log.info('WebScraperOperator Successful!')
