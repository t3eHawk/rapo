import datetime as dt
import pypyrus_logbook as logbook
import threading
import sqlalchemy as sql

from .database import db
from .executor import Executor
from .logger import logger
from .parser import Parser


class Control():
    def __init__(self, name, date_from=None, date_to=None, trigger=None):
        logger = logbook.logger()

        self.name = name
        self.trigger = trigger
        self.parser = Parser(self)
        self.executor = Executor(self)

        self.log = self.parser.parse_log()
        self.config = self.parser.parse_config()

        self.id = int(self.config.control_id)
        self.group = self.config.control_group
        self.type = self.config.control_type
        self.engine = self.config.control_engine
        self._process_id = None
        self._start_date = None
        self._end_date = None
        self._status = None
        self.date_from = self.parser.parse_date_from(date_from)
        self.date_to = self.parser.parse_date_to(date_to)
        self.fetched_x = None
        self.fetched_a = None
        self.fetched_b = None
        self.success_x = None
        self.success_a = None
        self.success_b = None
        self.errors_x = None
        self.errors_a = None
        self.errors_b = None
        self.error_level_x = None
        self.error_level_a = None
        self.error_level_b = None

        self.table_x = None
        self.table_a = None
        self.table_b = None
        self.table_fetx = None
        self.table_feta = None
        self.table_fetb = None
        self.table_errx = None
        self.table_erra = None
        self.table_errb = None
        pass

    def __str__(self):
        if hasattr(self, '_process_id') is False or self._process_id is None:
            return f'[{self.name}]'
        else:
            return f'[{self.name}:{self._process_id}]'

    __repr__ = __str__

    @property
    def process_id(self):
        return self._process_id

    @property
    def pid(self):
        return self._process_id

    @property
    def source_x(self):
        return self.parser.to_lower(self.config.source_x)

    @property
    def source_a(self):
        return self.parser.to_lower(self.config.source_a)

    @property
    def source_b(self):
        return self.parser.to_lower(self.config.source_b)

    @property
    def date_x(self):
        return self.parser.to_lower(self.config.date_x)

    @property
    def date_a(self):
        return self.parser.to_lower(self.config.date_a)

    @property
    def date_b(self):
        return self.parser.to_lower(self.config.date_b)

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value
        logger.info(f'{self} Status changed to {self._status}')
        pass

    @property
    def errors(self):
        errors = self.parser.parse_errors()
        return errors

    @property
    def output_x(self):
        output_x = self.parser.parse_output_x()
        return output_x

    @property
    def need_hook(self):
        return True if self.config.need_hook == 'Y' else False

    @property
    def need_b(self):
        return True if self.config.need_b == 'Y' else False

    def run(self):
        logger.debug(f'{self} Running control...')
        is_initiated = self._initiate()
        if is_initiated is True:
            is_started = self._start()
            if is_started is True:
                is_progressed = self._progress()
                if is_progressed is True:
                    is_finished = self._finish()
        self._done()
        self._hook()
        pass

    def _initiate(self):
        logger.info(f'{self} Initiating control...')
        try:
            self.status = 'I'
            logger.debug(f'{self} Creating new record in {self.log}')
            conn = db.connect()
            insert = self.log.insert()
            insert = insert.values(control_id=self.id,
                                   added=dt.datetime.now(),
                                   status=self.status,
                                   date_from=self.date_from,
                                   date_to=self.date_to)
            result = conn.execute(insert)
            self._process_id = int(result.inserted_primary_key[0])
        except:
            logger.error()
            return False
        else:
            logger.debug(f'{self} New record in {self.log} '
                         f'with primary_key={self._process_id} created')
            logger.info(f'{self} Control owns PROCESS_ID[{self._process_id}]')
            logger.info(f'{self} Control initiated')
            return True

    def _start(self):
        logger.info(f'{self} Starting control...')
        try:
            self.status = 'S'
            self._start_date = dt.datetime.now()
            self._update(status=self._status, start_date=self._start_date)
            if self.type in ('analysis', 'kpi', 'mismatch'):
                self.table_x = self.parser.parse_table_x()
        except:
            logger.error()
            return False
        else:
            logger.info(f'{self} Control started at {self.start_date}')
            return True

    def _progress(self):
        try:
            self.status = 'P'
            self._update(status=self._status)
            self._fetch()
            self._execute()
            self._save()
        except:
            logger.error()
            return False
        else:
            return True

    def _finish(self):
        logger.info(f'{self} Finishing control...')
        try:
            self.status = 'F'
            self._update(status=self._status)
            self.executor.drop_temporary_tables()
        except:
            logger.error()
            return False
        else:
            logger.info(f'{self} Control finished')
            return True

    def _fetch(self):
        logger.info(f'{self} Fetching records...')
        conn = db.connect()
        if self.type in ('analysis', 'kpi', 'mismatch'):
            self.table_fetx = self.executor.fetch_source_x()
            count = sql.select([sql.func.count()])\
                       .select_from(self.table_fetx)
            self.fetched_x = conn.execute(count).scalar()
            logger.info(f'{self} Records fetched from X: {self.fetched_x}')
            self._update(fetched_x=self.fetched_x)
        pass

    def _execute(self):
        logger.info(f'{self} Executing control...')
        conn = db.connect()
        if self.type == 'analysis':
            if self.fetched_x or 0 > 0:
                self.table_errx = self.executor.analyze()
                count = sql.select([sql.func.count()])\
                           .select_from(self.table_errx)
                self.errors_x = conn.execute(count).scalar()
                self.success_x = self.fetched_x-self.errors_x
                self.error_level_x = (self.errors_x/self.fetched_x)*100
                self._update(errors_x=self.errors_x,
                             success_x=self.success_x,
                             error_level_x=self.error_level_x)
        elif self.type == 'kpi':
            pass
        elif self.type == 'mismatch':
            pass
        elif self.type == 'missing':
            pass
        logger.info(f'{self} Control executed')
        pass

    def _save(self):
        logger.info(f'{self} Saving results...')
        if self.type == 'analysis':
            if self.errors_x or 0 > 0:
                self.executor.save_x()
        if self.type == 'kpi':
            pass
        if self.type == 'mismatch':
            pass
        if self.type == 'missing':
            pass
        logger.info(f'{self} Results saved')
        pass

    def _done(self):
        try:
            self.status = 'D' if logger.with_error is False else 'E'
            self._end_date = dt.datetime.now()
            self._update(status=self._status, end_date=self._end_date)
        except:
            logger.error()
            return False
        else:
            logger.info(f'{self} ended at {self.end_date}')
            return True

    def _hook(self):
        if self.need_hook is True:
            self.executor.hook()
        pass

    def _update(self, **kwargs):
        logger.debug(f'{self} Updating {self.log} with {kwargs}')
        conn = db.connect()
        update = self.log.update()
        update = update.values(**kwargs, updated_date=dt.datetime.now())
        update = update.where(self.log.c.process_id == self._process_id)
        conn.execute(update)
        logger.debug(f'{self} {self.log} updated')
        pass
