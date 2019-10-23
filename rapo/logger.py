import pypyrus_logbook as logbook

from .config import config


logger = logbook.logger(file=True)
logger.configure(format='{isodate}\t{thread}\t{rectype}\t{message}\n')
