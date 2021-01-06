# helpers/scraper/unece_ports/spiders/lib/utils.py


from collections import namedtuple
import pandas as pd
from helpers.scraper.unece_ports.item_loaders.processors import (
    _DEFAULT_VALUE
)


EXPECTED_COLUMNS = ['NameWoDiacritics', 'LOCODE', 'Coordinates']
SEARCH_COLUMN = 'Function'


def get_data(response_body) -> namedtuple:
    """
    Parse table elements from response body and store into DataFrames.

    :param bytes response_body:
        HTML response body that will be searched for table elements.
    :return namedtuple Ports:
        namedtuple instance that contains an iterable and string attribute
    """
    _, df_country, df = pd.read_html(response_body)
    if len(df) and len(df_country):
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
    return 0


def filter_dataframe(_df, _filter='') -> pd.DataFrame:
    """Filter null values from a DataFrame, with an optional filter.
    Filtering expected columns and expected search values.

    :param DataFrame _df:
        DataFrame to be filtered.
    :param str _filter:
        str used to filter SEARCH_COLUMN constant
    :return DataFrame _df:
        filtered DataFrame
    """
    _df = _df.fillna(_DEFAULT_VALUE)
    _df = _df[_df[SEARCH_COLUMN].str.contains(_filter)]
    _df = _df[EXPECTED_COLUMNS]
    return _df
