"""Contains class used as an interface of RAPO control."""

import datetime as dt
import threading as th
import traceback

from .database import db
from .executor import Executor
from .logger import logger
from .parser import Parser
from .utils import utils


class Control():
    """Represents RAPO control.

    Contains all elements needed to get all necessary control information and
    run it.

    Parameters
    ----------
    name : str
        Name of the control from RAPO_CONFIG.
    date_from : str or datetime, optional
        Data source date lower bound.
    date_to : str or datetime, optional
        Data source date upper bound.

    Attributes
    ----------
    name : str
        Name of the control from RAPO_CONFIG.
    parser : rapo.Parser
        Parser instance for this control.
    executor : rapo.Executor
        Executor instance for this control.
    log : sqlalchemy.Table
        RAPO_LOG table object.
    config : sqlalchemy.RowProxy
        RAPO_CONFIG record for this control.
    id : int
        ID of the control from RAPO_CONFIG.
    group : str or None
        Group of the control from RAPO_CONFIG.
    type : str or None
        Type of the control from RAPO_CONFIG. See RAPO_REF_TYPES table.
    method: str or None
        Method of the control from RAPO_CONFIG. See RAPO_REF_METHODS table.
    engine : str or None
        Engine of the control from RAPO_CONFIG. See RAPO_REF_ENGINES table.
    process_id : int or None
        Process id from RAPO_LOG.
    pid : int or None
        Process id from RAPO_LOG.
    source_name: str or None
        Data source name.
    source_date_field :
        Data source date field.
    source_name_a : str or None
        Data source A name.
    source_date_field_a :
        Data source A date field.
    source_name_b : str or None
        Data source B name.
    source_date_field_b :
        Data source B date field.
    updated : datetime or None
        Date when process RAPO_LOG record was last updated.
    start_date : datetime or None
        Date when process started.
    end_date : datetime or None
        Date when process finished.
    status : bool or None
        Current process status.
    select : sqlalchemy.Select
        SQL statement to fetch data from data source.
    select_a : sqlalchemy.Select
        SQL statement to fetch data from data source A.
    select_b : sqlalchemy.Select
        SQL statement to fetch data from data source B.
    match_config : list
        Match configuration that present how data sources must be matched.
    mismatch_config : list
        Mismatch configuration that define when mismatch is met.
    error_config : list
        Error configuration that define when error is met.
    output_columns : list or None
        Output column configuration.
    output_columns_a : list or None
        Output A column configuration.
    output_columns_b : list or None
        Output B column configuration.
    need_a : bool or None
        Flag to define whether save output A or not.
    need_b : bool or None
        Flag to define whether save output B or not.
    need_hook : bool or None
        Flag to define whether run database hook procedure or not.
    trigger : float or None
        Date trigger of this process.
    table_source : sqlalchemy.Table
        Proxy object reflecting data source table.
    table_source_a : sqlalchemy.Table
        Proxy object reflecting data source A table.
    table_source_b : sqlalchemy.Table
        Proxy object reflecting data source B table.
    table_fetched : sqlalchemy.Table
        Proxy object reflecting table with fetched records from data source.
    table_fetched_a : sqlalchemy.Table
        Proxy object reflecting table with fetched records from data source A.
    table_fetched_b : sqlalchemy.Table
        Proxy object reflecting table with fetched records from data source B.
    table_errors : sqlalchemy.Table
        Proxy object reflecting table with found discrapancies.
    table_errors_a : sqlalchemy.Table
        Proxy object reflecting table with found discrapancies from A.
    table_errors_b : sqlalchemy.Table
        Proxy object reflecting table with found discrapancies from B.
    table_matched : sqlalchemy.Table
        Proxy object reflecting table with found matches.
    table_mismatched : sqlalchemy.Table
        Proxy object reflecting table with found mismatches.
    fetched : int or None
        Number of fetched records from data source.
    success : int or None
        Number of success results.
    errors : int or None
        Number of discrapancies.
    error_level : float or None
        Indicator presenting the percentage of discrapancies among the fetched
        records.
    fetched_a : int or None
        Number of fetched records from data source A.
    fetched_b : int or None
        Number of fetched records from data source B.
    success_a : int or None
        Number of success results in A.
    success_b : int or None
        Number of success results in B.
    errors_a : int or None
        Number of discrapancies in A.
    errors_b : int or None
        Number of discrapancies in B.
    error_level_a : float or None
        Indicator presenting the percentage of discrapancies among the fetched
        records from A.
    error_level_b : float or None
        Indicator presenting the percentage of discrapancies among the fetched
        records from B.
    date_from : datetime
        Data source date lower bound.
    date_from : datetime
        Data source date upper bound.
    """

    def __init__(self, name, date_from=None, date_to=None, _trigger=None):
        self.name = name
        self._trigger = _trigger
        self.parser = Parser(self)
        self.executor = Executor(self)

        self.log = self.parser.parse_log()
        self.config = self.parser.parse_config()

        self.id = int(self.config.control_id)
        self.group = self.config.control_group
        self.type = self.config.control_type
        self.method = self.config.control_method
        self.engine = self.config.control_engine
        self._process_id = None
        self._updated = None
        self._start_date = None
        self._end_date = None
        self._status = None
        self.date_from = self.parser.parse_date_from(date_from)
        self.date_to = self.parser.parse_date_to(date_to)

        self.table_source = None
        self.table_source_a = None
        self.table_source_b = None

        self.table_fetched = None
        self.table_fetched_a = None
        self.table_fetched_b = None

        self.table_errors = None
        self.table_errors_a = None
        self.table_errors_b = None

        self.table_matched = None
        self.table_mismatched = None

        self.fetched = None
        self.success = None
        self.errors = None
        self.error_level = None

        self.fetched_a = None
        self.fetched_b = None
        self.success_a = None
        self.success_b = None
        self.errors_a = None
        self.errors_b = None
        self.error_level_a = None
        self.error_level_b = None

        self._with_error = False
        self._last_error = None
        pass

    def __str__(self):
        """Take control information and represent it as a simple string.

        Returns
        -------
        value : str
            Control name with or withoud process id.
        """
        if hasattr(self, '_process_id') is False or self._process_id is None:
            return f'[{self.name}]'
        else:
            return f'[{self.name}:{self._process_id}]'

    __repr__ = __str__

    @property
    def process_id(self):
        """Get control process id from RAPO_LOG."""
        return self._process_id

    @property
    def pid(self):
        """Get control process id from RAPO_LOG."""
        return self._process_id

    @property
    def source_name(self):
        """Get control data source name."""
        return utils.to_lower(self.config.source_name)

    @property
    def source_date_field(self):
        """Get control data source date field."""
        return utils.to_lower(self.config.source_date_field)

    @property
    def source_name_a(self):
        """Get control data source A name."""
        return utils.to_lower(self.config.source_name_a)

    @property
    def source_date_field_a(self):
        """Get control data source A date field."""
        return utils.to_lower(self.config.source_date_field_a)

    @property
    def source_name_b(self):
        """Get control data source B name."""
        return utils.to_lower(self.config.source_name_b)

    @property
    def source_date_field_b(self):
        """Get control data source B date field."""
        return utils.to_lower(self.config.source_date_field_b)

    @property
    def updated(self):
        """Get control run record updated date."""
        return self._updated

    @property
    def start_date(self):
        """Get control run start date."""
        return self._start_date

    @property
    def end_date(self):
        """Get control run end date."""
        return self._end_date

    @property
    def status(self):
        """Get control run status."""
        return self._status

    @status.setter
    def status(self, value):
        """Set control run status."""
        self._status = value
        self._updated = dt.datetime.now()
        logger.info(f'{self} Status changed to {self._status}')
        pass

    @property
    def select(self):
        """Get SQL statement to fetch data from data source."""
        return self.parser.parse_select()

    @property
    def select_a(self):
        """Get SQL statement to fetch data from data source A."""
        return self.parser.parse_select_a()

    @property
    def select_b(self):
        """Get SQL statement to fetch data from data source B."""
        return self.parser.parse_select_b()

    @property
    def match_config(self):
        """Get control match configuration."""
        config = self.parser.parse_match_config()
        return config

    @property
    def mismatch_config(self):
        """Get control mismatch configuration."""
        return self.parser.parse_mismatch_config()

    @property
    def error_config(self):
        """Get control error configuration."""
        return self.parser.parse_error_config()

    @property
    def output_columns(self):
        """Get control output column configuration."""
        return self.parser.parse_output_columns()

    @property
    def output_columns_a(self):
        """Get control output A column configuration."""
        return self.parser.parse_output_columns_a()

    @property
    def output_columns_b(self):
        """Get control output B column configuration."""
        return self.parser.parse_output_columns_b()

    @property
    def need_a(self):
        """Get parameter defining necessity of data source A saving."""
        return True if self.config.need_a == 'Y' else False

    @property
    def need_b(self):
        """Get parameter defining necessity of data source B saving."""
        return True if self.config.need_b == 'Y' else False

    @property
    def need_hook(self):
        """Get parameter defining necessity of hook execution."""
        return True if self.config.need_hook == 'Y' else False

    @property
    def trigger(self):
        """Get control date trigger."""
        return self._trigger

    def run(self):
        """Run control step by step."""
        logger.debug(f'{self} Running control...')
        is_initiated = self._initiate()
        if is_initiated is True:
            is_started = self._start()
            if is_started is True:
                is_in_progress = self._progress()
                if is_in_progress is True:
                    self._finish()
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
        except Exception:
            logger.error()
            self._error()
            return False
        else:
            logger.debug(f'{self} New record in {self.log} created')
            logger.info(f'{self} Control owns process_id {self._process_id}')
            logger.info(f'{self} Control initiated')
            return True

    def _start(self):
        logger.info(f'{self} Starting control...')
        try:
            self.status = 'S'
            self._start_date = dt.datetime.now()
            self._update(status=self._status, start_date=self._start_date)
            if self.type == 'ANL':
                self.table_source = self.parser.parse_table_source()
            if self.type == 'REC':
                self.table_source_a = self.parser.parse_table_source_a()
                self.table_source_b = self.parser.parse_table_source_b()
        except Exception:
            logger.error()
            self._error()
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
        except Exception:
            logger.error()
            self._error()
            return False
        else:
            return True

    def _finish(self):
        logger.info(f'{self} Finishing control...')
        try:
            self.status = 'F'
            self._update(status=self._status)
            self.executor.drop_temporary_tables()
        except Exception:
            logger.error()
            self._error()
            return False
        else:
            logger.info(f'{self} Control finished')
            return True

    def _fetch(self):
        logger.info(f'{self} Fetching records...')
        if self.type == 'ANL':
            logger.info(f'{self} Fetching {self.source_name}...')
            self.table_fetched = self.executor.fetch_records()
            self.fetched = self.executor.count_fetched()
            logger.info(f'{self} Records fetched: {self.fetched}')
            self._update(fetched=self.fetched)
        if self.type == 'REC':
            threads = []
            current = th.current_thread()
            for attr in ('_fetch_a', '_fetch_b'):
                name = f'{current.name}({attr[1:]})'
                func = getattr(self, attr)
                thread = th.Thread(target=func, name=name, daemon=True)
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join()
                if self._last_error is not None:
                    raise self._last_error
            self._update(fetched_a=self.fetched_a, fetched_b=self.fetched_b)
        pass

    def _fetch_a(self):
        try:
            self.__fetch_a()
        except Exception as e:
            self._last_error = e
        pass

    def __fetch_a(self):
        logger.info(f'{self} Fetching {self.source_name_a}...')
        self.table_fetched_a = self.executor.fetch_records_a()
        self.fetched_a = self.executor.count_fetched_a()
        logger.info(f'{self} Records fetched A: {self.fetched_a}')
        pass

    def _fetch_b(self):
        try:
            self.__fetch_b()
        except Exception as e:
            self._last_error = e
        pass

    def __fetch_b(self):
        logger.info(f'{self} Fetching {self.source_name_b}...')
        self.table_fetched_b = self.executor.fetch_records_b()
        self.fetched_b = self.executor.count_fetched_b()
        logger.info(f'{self} Records fetched B: {self.fetched_b}')
        pass

    def _execute(self):
        logger.info(f'{self} Executing control...')
        if self.type == 'ANL':
            if self.fetched or 0 > 0:
                self.table_errors = self.executor.analyze()
                self.errors = self.executor.count_errors()
                self.success = self.fetched-self.errors
                self.error_level = (self.errors/self.fetched)*100
                self._update(errors=self.errors,
                             success=self.success,
                             error_level=self.error_level)
        if self.type == 'REC' and self.method == 'MA':
            threads = []
            current = th.current_thread()
            for attr in ('_match', '_mismatch'):
                name = f'{current.name}({attr[1:]})'
                func = getattr(self, attr)
                thread = th.Thread(target=func, name=name, daemon=True)
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join()
                if self._last_error is not None:
                    raise self._last_error

            self.error_level = (self.errors/(self.success+self.errors))*100
            self._update(errors=self.errors,
                         success=self.success,
                         error_level=self.error_level)
        logger.info(f'{self} Control executed')
        pass

    def _match(self):
        try:
            self.__match()
        except Exception as e:
            self._last_error = e
        pass

    def __match(self):
        self.table_matched = self.executor.match()
        self.success = self.executor.count_matched()
        pass

    def _mismatch(self):
        try:
            self.__mismatch()
        except Exception as e:
            self._last_error = e
        pass

    def __mismatch(self):
        self.table_mismatched = self.executor.mismatch()
        self.errors = self.executor.count_mismatched()
        pass

    def _save(self):
        logger.info(f'{self} Saving results...')
        if self.type == 'ANL':
            if self.errors or 0 > 0:
                self.executor.save_errors()
        if self.type == 'REC' and self.method == 'MA':
            if self.errors or 0 > 0:
                self.executor.save_mismatched()
        logger.info(f'{self} Results saved')
        pass

    def _done(self):
        try:
            self.status = 'D' if self._with_error is False else 'E'
            self._end_date = dt.datetime.now()
            self._update(status=self._status, end_date=self._end_date)
        except Exception:
            logger.error()
            self._error()
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
        update = update.values(**kwargs, updated=dt.datetime.now())
        update = update.where(self.log.c.process_id == self._process_id)
        conn.execute(update)
        logger.debug(f'{self} {self.log} updated')
        pass

    def _error(self):
        self._with_error = True
        self._update(text_error=traceback.format_exc())
        pass
