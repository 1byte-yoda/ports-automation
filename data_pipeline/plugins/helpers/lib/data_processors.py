import re
import math
from unicodedata import normalize


_DEFAULT_VALUE = 'missing_value'


class PortsItemProcessor:

    def process_item(self, _item: dict):
        _item['coordinates'] = self.process_coordinates(
            _item['coordinates']
        )
        _item['countryName'] = self.process_country_name(
            _item['countryName']
        )
        _item['portName'] = self.process_port_name(
            _item['portName']
        )
        _item['unlocode'] = self.process_unlocode(
            _item['unlocode']
        )
        return _item

    def process_coordinates(self, coordinates: str) -> str:
        if not coordinates or self._is_nan(coordinates):
            return _DEFAULT_VALUE
        coordinates = normalize('NFKD', coordinates)
        coordinates = coordinates.strip()
        return coordinates if coordinates else _DEFAULT_VALUE

    def process_country_name(self, country_name: str) -> str:
        if not country_name or self._is_nan(country_name):
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

    def process_port_name(self, port_name: str) -> str:
        if not port_name or self._is_nan(port_name):
            return _DEFAULT_VALUE
        port_name = normalize('NFKD', port_name)
        port_name = port_name.strip()
        port_name = port_name.title()
        return port_name if port_name else _DEFAULT_VALUE

    def process_unlocode(self, unlocode: str) -> str:
        if not unlocode or self._is_nan(unlocode):
            return _DEFAULT_VALUE
        unlocode = normalize('NFKD', unlocode)
        unlocode = unlocode.strip()
        unlocode = " ".join(unlocode.split())
        unlocode = unlocode.upper()
        return unlocode if unlocode else _DEFAULT_VALUE

    def _is_nan(self, x):
        return isinstance(x, float) and math.isnan(x)
