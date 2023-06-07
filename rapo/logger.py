"""Contains application logger."""

import pepperoni

from .config import config


logger = pepperoni.logger(file=True)
logger.configure(format='{isodate}\t{thread}\t{rectype}\t{message}\n')

if config.check('LOGGING'):
    parameters = config['LOGGING']
    logger.configure(**parameters)
