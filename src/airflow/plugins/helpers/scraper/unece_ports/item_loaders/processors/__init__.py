# helpers/scraper/unece_ports/item_loaders/processors/__init__.py


import re
from unicodedata import normalize


_DEFAULT_VALUE = 'missing_value'
_NO_AVAILABLE_PORT = 'no_available_port'
DEFAULT_ITEM = {
    'portName': _NO_AVAILABLE_PORT,
    'coordinates': _NO_AVAILABLE_PORT,
    'unlocode': _NO_AVAILABLE_PORT
}


def process_coordinates(coordinates: str) -> str:
    if not coordinates:
        return _DEFAULT_VALUE
    coordinates = normalize('NFKD', coordinates)
    coordinates = coordinates.strip()
    return coordinates if coordinates else _DEFAULT_VALUE


def process_country_name(country_name: str) -> str:
    if not country_name:
        return _DEFAULT_VALUE
    country_code_pattern = r'\([A-Z]{2}\)'
    country_name = normalize('NFKD', country_name)
    country_name = re.sub(
        pattern=country_code_pattern,
        repl='',
        string=country_name
    ).strip()
    country_name = country_name.title()
    country_name.replace('And', 'and')
    return country_name if country_name else _DEFAULT_VALUE


def process_port_name(port_name: str) -> str:
    if not port_name:
        return _DEFAULT_VALUE
    port_name = normalize('NFKD', port_name)
    port_name = port_name.strip()
    return port_name if port_name else _DEFAULT_VALUE


def process_unlocodo(unlocodo: str) -> str:
    if not unlocodo:
        return _DEFAULT_VALUE
    unlocodo = normalize('NFKD', unlocodo)
    unlocodo = unlocodo.strip()
    return unlocodo if unlocodo else _DEFAULT_VALUE
