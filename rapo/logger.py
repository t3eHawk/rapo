"""Contains application logger."""

import pepperoni

from .config import config


logger = pepperoni.logger(file=True)
logger.configure(format='{isodate}\t{thread}\t{rectype}\t{message}\n')

if config.has_section('LOGGING'):
    LOGGING = config['LOGGING']
    logger.configure(console=LOGGING.getboolean('console'),
                     file=LOGGING.getboolean('file'),
                     info=LOGGING.getboolean('info'),
                     debug=LOGGING.getboolean('debug'),
                     error=LOGGING.getboolean('error'),
                     warning=LOGGING.getboolean('warning'),
                     critical=LOGGING.getboolean('critical'),
                     maxsize=LOGGING.getint('maxsize'),
                     maxdays=LOGGING.getint('maxdays'))
