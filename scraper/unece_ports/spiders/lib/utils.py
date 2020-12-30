# scraper/unece_ports/spiders/lib/utils.py


from collections import namedtuple
import pandas as pd
from unece_ports.item_loaders.processors import _DEFAULT_VALUE


EXPECTED_COLUMNS = ['NameWoDiacritics', 'LOCODE', 'Coordinates']
SEARCH_COLUMN = 'Function'


def get_data(response_body: bytes) -> namedtuple:
    """Parse table element from response body and store into a DataFrame.
    Filtering expected columns and expected search values.
    """
    _, _, df = pd.read_html(response_body)
    if len(df):
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
                field_names=['iter']
            )
            return Ports(
                iter=df.iterrows()
            )
    return 0
