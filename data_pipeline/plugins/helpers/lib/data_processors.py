# data_pipeline/plugins/helpers/lib/data_processors.py


import re
import math
from unicodedata import normalize


_DEFAULT_VALUE = 'missing_value'


class PortsItemProcessor:
    """
    Processor that runs various functions against items in a dictionary.
    To standardize data, this class was expected to clean each item
    which depends on the context of the problem.
    """

    def process_item(self, _item: dict):
        """Process each key-value pair in a given dictionary.

        :param dict _item:
            Contains key-value pair to be processed.
        :return dict _item:
            The processed/cleaned dictionary.
        """
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
        """Process coordinates value.

        This function will:
            - Remove unicode characters
            - Remove excess spaces/escape characters.
            - Replace null value with the _DEFAULT_VALUE.

        Example input coordinates: '3639N\x0006659E '
        Example output coordinates: '3639N 06659E'

        :param str coordinates:
            Geographical coordinates that will be processed.
        :return str coordinates:
            The processed/cleaned coordinates.
        """
        if not coordinates or self._is_nan(coordinates):
            return _DEFAULT_VALUE
        coordinates = normalize('NFKD', coordinates)
        coordinates = coordinates.strip()
        return coordinates if coordinates else _DEFAULT_VALUE

    def process_country_name(self, country_name: str) -> str:
        """Process country_name value.

        This function will:
            - Removes the country code using regex pattern.
            - Remove unicode characters.
            - Remove excess spaces/escape characters.
            - Replace null value with the _DEFAULT_VALUE.
            - Convert country_name to title case.

        Example input country_name: '(AF)\x00Afghanistan '
        Example output country_name: 'Afghanistan'

        :param str country_name:
            The country name that will be processed.
        :return str country_name:
            The processed/cleaned country name.
        """
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
        """Process port_name value.

        This function will:
            - Remove unicode characters.
            - Remove excess spaces/escape characters.
            - Replace null value with the _DEFAULT_VALUE.
            - Convert port_name to title case.

        Example input port_name: ' Dehdadi\x00'
        Example output port_name: 'Dehdadi'

        :param str port_name:
            The port name that will be processed.
        :return str port_name:
            The processed/cleaned port name.
        """
        if not port_name or self._is_nan(port_name):
            return _DEFAULT_VALUE
        port_name = normalize('NFKD', port_name)
        port_name = port_name.strip()
        port_name = port_name.title()
        return port_name if port_name else _DEFAULT_VALUE

    def process_unlocode(self, unlocode: str) -> str:
        """Process unlocode value.

        This function will:
            - Remove unicode characters.
            - Remove excess spaces/escape characters.
            - Replace null value with the _DEFAULT_VALUE.
            - Convert unlocode to upper case.

        Example input unlocode: ' AF\x00DHD\x00'
        Example output unlocode: 'AF DHD'

        :param str unlocode:
            The unlocode that will be processed.
        :return str unlocode:
            The processed/cleaned unlocode.
        """
        if not unlocode or self._is_nan(unlocode):
            return _DEFAULT_VALUE
        unlocode = normalize('NFKD', unlocode)
        unlocode = unlocode.strip()
        unlocode = " ".join(unlocode.split())
        unlocode = unlocode.upper()
        return unlocode if unlocode else _DEFAULT_VALUE

    def _is_nan(self, x: any) -> bool:
        """Helper function to determine if a variable x is null or not.

        :param any x:
            the variable that was suspected to be null.
        :return bool:
            whether x is null or not.
        """
        return isinstance(x, float) and math.isnan(x)
