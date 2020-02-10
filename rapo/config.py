"""Configurator.

Contains configurator used to transfer parameters from user to application.
"""

import configparser
import os


path = os.path.abspath(os.path.expanduser('~/.rapo/rapo.ini'))
config = configparser.ConfigParser(allow_no_value=True)
config.read(path)
