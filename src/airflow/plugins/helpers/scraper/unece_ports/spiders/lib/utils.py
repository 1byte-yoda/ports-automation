# helpers/scraper/unece_ports/spiders/lib/utils.py


from collections import namedtuple
import pandas as pd
from pandas import DataFrame
from helpers.scraper.unece_ports.item_loaders.processors import (
    _DEFAULT_VALUE
)


EXPECTED_COLUMNS = ['NameWoDiacritics', 'LOCODE', 'Coordinates']
SEARCH_COLUMN = 'Function'


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
        raise ValueError('not enough values to unpack DataFrame')
    dfs_not_empty = len(df) and len(df_country)
    if dfs_not_empty:
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
        and without null like values
    """
    _df = _df.fillna(_DEFAULT_VALUE)
    _df = _df[_df[SEARCH_COLUMN].str.contains(_filter)]
    _df = _df[EXPECTED_COLUMNS]
    return _df
