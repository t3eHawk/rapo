"""Contains application configurator.

Used to transfer parameters from user to application.
"""

import configparser
import os


path = os.path.abspath(os.path.expanduser('~/.rapo/rapo.ini'))
config = configparser.ConfigParser(allow_no_value=True)
config.read(path)


def get_deprecated(parameters, used, required):
    """Get the parameter, considering the depreciations."""
    parameter = parameters.get(required)
    if not parameter:
        if parameters.get(used):
            parameter = parameters.get(used)
            print(f'Please use parameter [{required}] instead of',
                  f'[{used}], which is deprecated and will be',
                  f'removed soon from [{parameters.name}] section',
                  f'of {path} configuration file!')
    return parameter
