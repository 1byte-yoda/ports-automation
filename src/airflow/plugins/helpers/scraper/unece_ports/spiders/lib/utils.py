# helpers/scraper/unece_ports/spiders/lib/utils.py


from logging import Logger
from collections import namedtuple
import pandas as pd
from pandas import DataFrame


EXPECTED_COLUMNS = ['NameWoDiacritics', 'LOCODE', 'Coordinates']
SEARCH_COLUMN = 'Function'


logger = Logger(__file__)


def get_data(response_body: bytes) -> namedtuple:
    """
    Parse table elements from response body and store into DataFrames.

    :param bytes response_body:
        HTML response body that will be searched for table elements.
    :return namedtuple Ports:
        namedtuple instance that contains an iterable and string attribute
    """
    try:
        _, df_country, df = pd.read_html(response_body)
    except ValueError:
        logger.info(
            'Cannot scrape this website. '
            'Tables to be scraped has changed its structure. '
            'Please check the source website.'
        )
        return 0
    country_name = df_country.iloc[0][0]
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])
    valid_columns = (
        column in list(df.columns)
        for column in EXPECTED_COLUMNS + [SEARCH_COLUMN]
    )
    if all(valid_columns):
        df = filter_dataframe(df, "1")
        Ports = namedtuple(
            typename='Ports',
            field_names=['iter', 'countryName']
        )
        return Ports(
            iter=df.iterrows(),
            countryName=country_name
        )


def filter_dataframe(_df: DataFrame, _filter: str = '') -> DataFrame:
    """Filter null values from a DataFrame, with an optional filter.
    Filtering expected columns and expected search values.

    :param DataFrame _df:
        DataFrame to be filtered.
    :param str _filter:
        string to be used on filtering df[SEARCH_COLUMN],
        default is a blank string.
    :return DataFrame _df:
        filtered DataFrame with the expected columns
    """
    _df = _df[_df[SEARCH_COLUMN].str.contains(_filter)]
    _df = _df[EXPECTED_COLUMNS]
    return _df
