# data_pipeline/scraper/unece_ports/spiders/lib/utils.py


from logging import Logger
from collections import namedtuple
import pandas as pd
from pandas import DataFrame
from scrapy.http.response import Response


EXPECTED_COLUMNS = ['NameWoDiacritics', 'LOCODE', 'Coordinates']
SEARCH_COLUMN = 'Function'
DEFAULT_VALUE = 'missing_value'


logger = Logger('warner')


def get_data(response: Response) -> namedtuple:
    """
    Parse table elements from response body and store into DataFrames.

    :param bytes response_body:
        HTML response body that will be searched for table elements.
    :return namedtuple Ports:
        namedtuple instance that contains an iterable and string attribute
    """
    try:
        response_body = response.body
        _, df_country, df = pd.read_html(response_body)
    except ValueError:
        logger.warning(
            f'SKIPPING: {response.url}'
            'REASON: Number of tables has been changed. '
            'Using default values instead.'
        )
        return 0
    country_name = df_country.iloc[0][0]
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])
    all_expected_columns = EXPECTED_COLUMNS + [SEARCH_COLUMN]
    valid_columns = (
        column in list(df.columns)
        for column in all_expected_columns
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
    invalid_columns = [
        all_expected_columns[i] for i, v in enumerate(valid_columns)
    ]
    logger.warning(
        f'SKIPPING: {response.url}'
        'REASON: There are invalid columns found.\n'
        f'Invalid Columns: {invalid_columns}.'
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
    _df = _df.fillna(DEFAULT_VALUE)
    _df = _df[_df[SEARCH_COLUMN].str.contains(_filter)]
    _df = _df[EXPECTED_COLUMNS]
    return _df
