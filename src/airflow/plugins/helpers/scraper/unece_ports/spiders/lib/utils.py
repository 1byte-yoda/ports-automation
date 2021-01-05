# helpers/scraper/unece_ports/spiders/lib/utils.py


from collections import namedtuple
import pandas as pd
from helpers.scraper.unece_ports.item_loaders.processors import (
    _DEFAULT_VALUE
)


EXPECTED_COLUMNS = ['NameWoDiacritics', 'LOCODE', 'Coordinates']
SEARCH_COLUMN = 'Function'


def get_data(response_body: bytes) -> namedtuple:
    """
    Parse table element from response body and store into a DataFrame.
    """
    _, df_country, df = pd.read_html(response_body)
    if len(df) and len(df_country):
        country_name = df_country.iloc[0].values[0]
        df.columns = df.loc[0].values
        df = df.drop(0, axis=0)
        valid_columns = (
            column in list(df.columns)
            for column in EXPECTED_COLUMNS + [SEARCH_COLUMN]
        )
        if all(valid_columns):
            df = df.fillna(_DEFAULT_VALUE)
            df = df[df[SEARCH_COLUMN].str.contains('1')]
            df = df[EXPECTED_COLUMNS]
            Ports = namedtuple(
                typename='Ports',
                field_names=['iter', 'countryName']
            )
            return Ports(
                iter=df.iterrows(),
                countryName=country_name
            )
    return 0


def filter_dataframe(_df: pd.DataFrame, _filter: str = ''):
    """Filter null values from a DataFrame, with an optional filter.
    Filtering expected columns and expected search values.
    @param _df: DataFrame to be filtered.
    @param _filter: str used to filter SEARCH_COLUMN constant
    """
    _df = _df.fillna(_DEFAULT_VALUE)
    _df = _df[_df[SEARCH_COLUMN].str.contains(_filter)]
    _df = _df[EXPECTED_COLUMNS]
    return _df
