"""Contains application configurator.

Used to transfer parameters from user to application.
"""

import os
import re
import configparser


path = os.path.abspath(os.path.expanduser('~/.rapo/rapo.ini'))
encoding = 'utf-8'


class Configurator(dict):
    """Represents main configurator."""

    def __init__(self):
        super().__init__()
        if self.found:
            names = ['SCHEDULER', 'DATABASE', 'LOGGING', 'API']
            for name in names:
                self[name] = Configuration(name)
            self.load()

    def load(self):
        """Load configuration from file into memory."""
        parser = configparser.ConfigParser(allow_no_value=True)
        parser.read(path, encoding=encoding)
        for section in parser.sections():
            for option in parser.options(section):
                initial_value = parser[section][option]
                final_value = self.normalize(initial_value)
                self[section][option] = final_value
        return self

    def normalize(self, value):
        """Normalize given parameter value."""
        if value is None or value.upper() == 'NONE' or value.isspace():
            return None
        elif value.upper() == 'TRUE':
            return True
        elif value.upper() == 'FALSE':
            return False
        elif re.match(r'^[+-]?\d+$', value):
            return int(value)
        elif re.match(r'^[+-]?(\d*.\d+|\d+\.\d*)$', value):
            return float(value)
        return value

    def check(self, configuration_name):
        """Determine whether configuration presented or not."""
        if self.get(configuration_name) is not None:
            return True
        return False

    @property
    def found(self):
        """Determine whether configuration file found or not."""
        if os.path.exists(path):
            return True
        return False


class Configuration(dict):
    """Represents some configuration."""

    def __init__(self, name):
        super().__init__()
        self.name = name

    def __setitem__(self, key, value):
        """Set the parameter value."""
        super().__setitem__(key.lower(), value)

    def __getitem__(self, key):
        """Get the parameter value or raise an exception if not found."""
        return super().__getitem__(key.lower())

    def get(self, key, default=None):
        """Get the parameter value or return default if not found."""
        return super().get(key.lower(), default)

    def get_deprecated(self, used, required):
        """Get the parameter, considering the depreciations."""
        parameter = self.get(required)
        if not parameter:
            if self.get(used):
                parameter = self.get(used)
                print(f'Please use parameter [{required}] instead of',
                      f'[{used}], which is deprecated and will be',
                      f'removed soon from [{self.name}] section',
                      f'of {path} configuration file!')
        return parameter


config = Configurator()
