"""Contains RAPO control interface."""

import datetime as dt
import json
import multiprocessing as mp
import sys
import threading as th
import time as tm
import traceback as tb

import sqlalchemy as sa

from ..database import db
from ..logger import logger
from ..reader import reader
from ..utils import utils


class Control():
    """Represents certain RAPO control and acts like its API.

    Parameters
    ----------
    name : str, optional
        Name of the control from RAPO_CONFIG.
    timestamp : float or None
        Timestamp of this process.
    date_from : str or datetime, optional
        Data source date lower bound.
    date_to : str or datetime, optional
        Data source date upper bound.

    Attributes
    ----------
    name : str
        Name of the control from RAPO_CONFIG.
    timestamp : float or None
        Timestamp of this process.
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
    subtype: str or None
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
    rule_config : list
        Configuration defining success result.
    error_config : list
        Configuration defining error result.
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
    need_prerun_hook : bool or None
        Flag to define whether run database prerun hook function or not.
    need_hook : bool or None
        Flag to define whether run database hook procedure or not.
    source_table : sqlalchemy.Table
        Proxy object reflecting data source table.
    source_table_a : sqlalchemy.Table
        Proxy object reflecting data source A table.
    source_table_b : sqlalchemy.Table
        Proxy object reflecting data source B table.
    input_table : sqlalchemy.Table
        Proxy object reflecting table with fetched records from data source.
    input_table_a : sqlalchemy.Table
        Proxy object reflecting table with fetched records from data source A.
    input_table_b : sqlalchemy.Table
        Proxy object reflecting table with fetched records from data source B.
    result_table : sqlalchemy.Table
        Proxy object reflecting table with found matches.
    error_table : sqlalchemy.Table
        Proxy object reflecting table with found discrapancies.
    error_table_a : sqlalchemy.Table
        Proxy object reflecting table with found discrapancies from A.
    error_table_b : sqlalchemy.Table
        Proxy object reflecting table with found discrapancies from B.
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

    def __init__(self, name=None, timestamp=None, process_id=None,
                 date=None, date_from=None, date_to=None):
        self.name = name or reader.read_control_name(process_id)
        self.config = reader.read_control_config(self.name)
        self.result = reader.read_control_result(process_id)
        self.id = int(self.config['control_id'])
        self.group = self.config['control_group']
        self.type = self.config['control_type']
        self.subtype = self.config['control_subtype']
        self.engine = self.config['control_engine']
        self.timestamp = timestamp

        self.parser = Parser(self)
        self.executor = Executor(self)

        self.process = None
        self.handler = None

        self.process_id = process_id
        if self.result:
            self.start_date = self.result['start_date']
            self.end_date = self.result['end_date']
            self.updated = self.result['updated']
            self.status = self.result['status']

            self.date_from = self.result['date_from']
            self.date_to = self.result['date_to']

            self.fetched = self.result['fetched']
            self.success = self.result['success']
            self.errors = self.result['errors']
            self.error_level = self.result['error_level']

            self.fetched_a = self.result['fetched_a']
            self.fetched_b = self.result['fetched_b']
            self.success_a = self.result['success_a']
            self.success_b = self.result['success_b']
            self.errors_a = self.result['errors_a']
            self.errors_b = self.result['errors_b']
            self.error_level_a = self.result['error_level_a']
            self.error_level_b = self.result['error_level_b']
        else:
            self.start_date = None
            self.end_date = None
            self.updated = None
            self.status = None

            if self.timestamp:
                self.date_from, self.date_to = self.parser.parse_dates()
            elif date:
                self.date_from = self.parser.parse_date(date, 0, 0, 0)
                self.date_to = self.parser.parse_date(date, 23, 59, 59)
            else:
                self.date_from = self.parser.parse_date(date_from)
                self.date_to = self.parser.parse_date(date_to)

            self.fetched = None
            self.success = None
            self.errors = None
            self.error_level = None

            self.fetched_a = None
            self.success_a = None
            self.errors_a = None
            self.error_level_a = None

            self.fetched_b = None
            self.success_b = None
            self.errors_b = None
            self.error_level_b = None

        self.source_table = None
        self.source_table_a = None
        self.source_table_b = None

        self.input_table = None
        self.input_table_a = None
        self.input_table_b = None

        self.result_table = None
        self.error_table = None
        self.error_table_a = None
        self.error_table_b = None

        self.output_table = None
        self.output_table_a = None
        self.output_table_b = None

        self._with_error = False
        self._all_errors = []
        self._pending_error = None

    def __str__(self):
        """Take control information and represent it as a simple string.

        Returns
        -------
        value : str
            Control name with or withoud process id.
        """
        if self.process_id is None:
            return f'[{self.name}]'
        else:
            return f'[{self.name}:{self.process_id}]'

    __repr__ = __str__

    @property
    def name(self):
        """Get control name."""
        return self._name

    @name.setter
    def name(self, value):
        if isinstance(value, str) or value is None:
            self._name = value
        else:
            type = value.__class__.__name__
            message = f'name must be str or None, not {type}'
            raise TypeError(message)

    @property
    def process_id(self):
        """Get control run process ID."""
        return self._process_id

    @property
    def pid(self):
        """Shortcut for process_id."""
        return self._process_id

    @process_id.setter
    def process_id(self, value):
        if isinstance(value, int) or value is None:
            self._process_id = value
        else:
            type = value.__class__.__name__
            message = f'process_id must be int or None, not {type}'
            raise TypeError(message)

    @property
    def source_name(self):
        """Get control data source name."""
        return utils.to_lower(self.config['source_name'])

    @property
    def source_date_field(self):
        """Get control data source date field."""
        return utils.to_lower(self.config['source_date_field'])

    @property
    def source_name_a(self):
        """Get control data source A name."""
        return utils.to_lower(self.config['source_name_a'])

    @property
    def source_date_field_a(self):
        """Get control data source A date field."""
        return utils.to_lower(self.config['source_date_field_a'])

    @property
    def source_name_b(self):
        """Get control data source B name."""
        return utils.to_lower(self.config['source_name_b'])

    @property
    def source_date_field_b(self):
        """Get control data source B date field."""
        return utils.to_lower(self.config['source_date_field_b'])

    @property
    def status(self):
        """Get control run status."""
        return self._status

    @status.setter
    def status(self, value):
        """Set control run status."""
        if isinstance(value, str) and len(value) == 1:
            if hasattr(self, '_status'):
                self._status = value
                self.updated = dt.datetime.now()
                logger.info(f'{self} Status changed to {self._status}')
            else:
                self._status = value
        elif value is None:
            self._status = None
        else:
            message = f'incorrect status: {value}'
            raise ValueError(message)

    @property
    def initiated(self):
        """Check if control is initiated."""
        if self.status == 'I':
            return True
        else:
            return False

    @property
    def working(self):
        """Check if control is working."""
        if self.status in ('S', 'P', 'F'):
            return True
        else:
            return False

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
    def output_names(self):
        """Get output table names."""
        return self.parser.parse_output_names()

    @property
    def output_tables(self):
        """Get output tables."""
        return self.parser.parse_output_tables()

    @property
    def temp_names(self):
        """Get temporary table names."""
        return self.parser.parse_temp_names()

    @property
    def temp_tables(self):
        """Get temporary tables."""
        return self.parser.parse_temp_tables()

    @property
    def rule_config(self):
        """Get control match configuration."""
        if self.type == 'ANL':
            return []
        elif self.type == 'REC':
            return self.parser.parse_reconciliation_rule_config()
        elif self.type == 'REP':
            return []

    @property
    def error_config(self):
        """Get control error configuration."""
        if self.type == 'ANL':
            return self.parser.parse_analyze_error_config()
        elif self.type == 'REC':
            return self.parser.parse_reconciliation_error_config()
        elif self.type == 'REP':
            return []

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
        return True if self.config['need_a'] == 'Y' else False

    @property
    def need_b(self):
        """Get parameter defining necessity of data source B saving."""
        return True if self.config['need_b'] == 'Y' else False

    @property
    def need_hook(self):
        """Get parameter defining necessity of hook execution."""
        return True if self.config['need_hook'] == 'Y' else False

    @property
    def need_postrun_hook(self):
        """Get parameter defining necessity of hook execution."""
        return True if self.config['need_postrun_hook'] == 'Y' else False

    @property
    def need_prerun_hook(self):
        """Get parameter defining necessity of prerun hook execution."""
        return True if self.config['need_prerun_hook'] == 'Y' else False

    @property
    def text_error(self):
        """Get textual error representation of current control run."""
        errors = self._all_errors.copy()
        texts = [''.join(tb.format_exception(*error)) for error in errors]
        text = f'{str():->40}\n'.join(texts)
        return text

    def run(self):
        """Run control in an ordinary way."""
        logger.debug(f'{self} Running control...')
        if self._initiate():
            self._resume()

    def launch(self):
        """Run control as a separate accompanied stoppable process."""
        logger.debug(f'{self} Running control...')
        if self._initiate():
            self._spawn()

    def resume(self):
        """Resume initiated control run."""
        self._resume()

    def wait(self):
        """Wait untill control finishes."""
        return self._wait()

    def cancel(self):
        """Cancel control run."""
        if self.working:
            self._deinitiate()

    def delete(self):
        """Delete control results."""
        self._delete()

    def revoke(self):
        """Revoke control results."""
        self._revoke()

    def clean(self):
        """Clean control results."""
        self._clean()

    def _initiate(self):
        logger.info(f'{self} Initiating control...')
        try:
            self.status = 'I'
            logger.debug(f'{self} Creating new record in {db.tables.log}')
            insert = db.tables.log.insert()
            insert = insert.values(control_id=self.id,
                                   added=dt.datetime.now(),
                                   status=self.status,
                                   date_from=self.date_from,
                                   date_to=self.date_to)
            result = db.execute(insert)
            self.process_id = int(result.inserted_primary_key[0])
        except Exception:
            logger.error()
            return self._escape()
        else:
            logger.debug(f'{self} New record in {db.tables.log} created')
            logger.info(f'{self} Control owns process ID {self.process_id}')
            logger.info(f'{self} Control initiated')
            return self._continue()

    def _deinitiate(self):
        logger.info(f'{self} Deinitiating control...')
        try:
            self.status = None
            self._update(status=self.status)
        except Exception:
            logger.error()
        else:
            logger.info(f'{self} Control deinitiated')

    def _resume(self):
        if self.initiated:
            if self._prerun_hook():
                if self._start():
                    if self._progress():
                        if self._finish():
                            if self._done():
                                self._postrun_hook()

    def _spawn(self):
        logger.debug(f'{self} Spawning new process for the control...')
        context = mp.get_context('spawn')
        process = context.Process(name=self, target=self._resume)
        process.start()
        handler = th.Thread(name=f'{self}-Handler', target=self._handle)
        handler.start()
        logger.info(f'{self} Running as process on PID {process.pid}')
        self.process, self.handler = process, handler

    def _handle(self):
        process = self.process
        while process.is_alive() and self.status == 'P':
            control = Control(process_id=self.process_id)
            if not control.status:
                logger.info(f'{self} Control cancelation request received')
                self._terminate()
                self._cancel()
            tm.sleep(5)
            self.__dict__.update(control.__dict__)

    def _wait(self):
        process = self.process
        pid = process.pid
        logger.debug(f'{self} Waiting for process at PID {pid}...')
        while process.is_alive():
            tm.sleep(1)
        result_code = process.exitcode
        logger.debug(f'{self} Process at PID {pid} returns {result_code}')

    def _terminate(self):
        logger.info(f'{self} Terminating process of the control...')
        process = self.process
        pid = process.pid
        process.terminate()
        logger.info(f'{self} Process at PID {pid} terminated')

    def _start(self):
        logger.info(f'{self} Starting control...')
        try:
            self.status = 'S'
            self.start_date = dt.datetime.now()
            self._update(status=self.status, start_date=self.start_date)
            if self.type in ('ANL', 'REP'):
                self.source_table = self.parser.parse_source_table()
            elif self.type == 'REC':
                self.source_table_a = self.parser.parse_source_table_a()
                self.source_table_b = self.parser.parse_source_table_b()
        except Exception:
            logger.error()
            return self._escape()
        else:
            logger.info(f'{self} Control started at {self.start_date}')
            return self._continue()

    def _progress(self):
        try:
            self.status = 'P'
            self._update(status=self.status)
            self._fetch()
            self._execute()
            self._save()
        except Exception:
            logger.error()
            return self._escape()
        else:
            return self._continue()

    def _finish(self):
        logger.info(f'{self} Finishing control...')
        try:
            self.status = 'F'
            self._update(status=self.status)
            self.executor.drop_temporary_tables()
        except Exception:
            logger.error()
            return self._escape()
        else:
            logger.info(f'{self} Control finished')
            return self._continue()

    def _cancel(self):
        logger.info(f'{self} Canceling control...')
        try:
            self.status = 'C'
            self._update(status=self.status)
            self.executor.drop_temporary_tables()
            self.executor.delete_output_records()
        except Exception:
            logger.error()
        else:
            logger.info(f'{self} Control canceled')

    def _done(self):
        try:
            self.status = 'D'
            self.end_date = dt.datetime.now()
            self._update(status=self.status, end_date=self.end_date)
        except Exception:
            logger.error()
            return self._escape()
        else:
            logger.info(f'{self} ended at {self.end_date}')
            return self._continue()

    def _error(self):
        try:
            self.status = 'E'
            self.end_date = dt.datetime.now()
            self._update(status=self.status, end_date=self.end_date)
        except Exception:
            logger.error()
        else:
            logger.info(f'{self} ended with error at {self.end_date}')

    def _continue(self):
        return True

    def _escape(self):
        self._with_error = True
        self._all_errors.append(sys.exc_info())
        self._update(text_error=self.text_error)
        return self._error()

    def _fetch(self):
        logger.info(f'{self} Fetching records...')
        if self.type in ('ANL', 'REP'):
            logger.info(f'{self} Fetching {self.source_name}...')
            self.input_table = self.executor.fetch_records()
            self.fetched = self.executor.count_fetched()
            logger.info(f'{self} Records fetched: {self.fetched}')
            self._update(fetched=self.fetched)
        elif self.type == 'REC':
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
                if self._pending_error is not None:
                    raise self._pending_error
            self._update(fetched_a=self.fetched_a, fetched_b=self.fetched_b)

    def _fetch_a(self):
        try:
            self.__fetch_a()
        except Exception as error:
            self._pending_error = error

    def __fetch_a(self):
        logger.info(f'{self} Fetching {self.source_name_a}...')
        self.input_table_a = self.executor.fetch_records_a()
        self.fetched_a = self.executor.count_fetched_a()
        logger.info(f'{self} Records fetched A: {self.fetched_a}')

    def _fetch_b(self):
        try:
            self.__fetch_b()
        except Exception as error:
            self._pending_error = error

    def __fetch_b(self):
        logger.info(f'{self} Fetching {self.source_name_b}...')
        self.input_table_b = self.executor.fetch_records_b()
        self.fetched_b = self.executor.count_fetched_b()
        logger.info(f'{self} Records fetched B: {self.fetched_b}')

    def _execute(self):
        logger.info(f'{self} Executing control...')
        if self.type == 'ANL':
            if self.fetched or 0 > 0:
                self.error_table = self.executor.analyze()
                self.errors = self.executor.count_errors()
                self.success = self.fetched-self.errors
                self.error_level = (self.errors/self.fetched)*100
                self._update(errors=self.errors,
                             success=self.success,
                             error_level=self.error_level)
        elif self.type == 'REP':
            if self.fetched or 0 > 0:
                self.error_table = self.executor.analyze()
        elif self.type == 'REC' and self.subtype == 'MA':
            threads = []
            current = th.current_thread()
            for action in ('match', 'mismatch'):
                name = f'{current.name}({action})'
                func = getattr(self, f'_{action}')
                thread = th.Thread(target=func, name=name, daemon=True)
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join()
                if self._pending_error is not None:
                    raise self._pending_error
            self.error_level = (self.errors/(self.success+self.errors))*100
            self._update(errors=self.errors,
                         success=self.success,
                         error_level=self.error_level)
        logger.info(f'{self} Control executed')

    def _match(self):
        try:
            self.__match()
        except Exception as error:
            self._pending_error = error

    def __match(self):
        self.result_table = self.executor.match()
        self.success = self.executor.count_matched()

    def _mismatch(self):
        try:
            self.__mismatch()
        except Exception as error:
            self._pending_error = error

    def __mismatch(self):
        self.error_table = self.executor.mismatch()
        self.errors = self.executor.count_mismatched()

    def _save(self):
        logger.info(f'{self} Saving results...')
        if self.type == 'ANL':
            if self.errors or 0 > 0:
                self.executor.save_errors()
        elif self.type == 'REP':
            if self.fetched or 0 > 0:
                self.executor.save_errors()
        elif self.type == 'REC':
            if self.subtype == 'MA':
                if self.errors or 0 > 0:
                    self.executor.save_mismatches()
        logger.info(f'{self} Results saved')

    def _delete(self):
        logger.info(f'{self} Deleting results...')
        self.executor.delete_output_records()
        logger.info(f'{self} Results deleted')

    def _revoke(self):
        try:
            logger.info(f'{self} Revoking control...')
            self.status = 'X'
            self._update(status=self.status)
            self._delete()
        except Exception:
            logger.error()
        else:
            logger.info(f'{self} Control revoked')

    def _clean(self):
        logger.info(f'{self} Cleaning control results...')
        outdated_results = list(self.parser.parse_outdated_results())
        if outdated_results:
            for table, process_ids in outdated_results:
                for process_id in process_ids:
                    repr = f'[{self.name}:{process_id}]'
                    logger.info(f'{repr} Deleting results in {table}...')
                    id = table.c.rapo_process_id
                    query = table.delete().where(id == process_id)
                    text = db.formatter(query)
                    logger.debug(f'{self} Deleting from {table} '
                                 f'with query:\n{text}')
                    db.execute(query)
                    logger.info(f'{repr} Results in {table} deleted')
            logger.info(f'{self} Control results cleaned')
        else:
            logger.info(f'{self} No control results to clean')

    def _update(self, **kwargs):
        logger.debug(f'{self} Updating {db.tables.log} with {kwargs}')
        update = db.tables.log.update()
        update = update.values(**kwargs, updated=dt.datetime.now())
        update = update.where(db.tables.log.c.process_id == self.process_id)
        db.execute(update)
        logger.debug(f'{self} {db.tables.log} updated')

    def _prerun_hook(self):
        if self.need_hook and self.need_prerun_hook:
            hook_result, hook_code = self.executor.prerun_hook()
            if not hook_result:
                message = ('Control execution stopped because PRERUN HOOK ',
                           f'function evaluated as NOT OK [{hook_code}]')
                self._update(text_message=message)
                return False
        return True

    def _postrun_hook(self):
        if self.need_hook and self.need_postrun_hook:
            self.executor.postrun_hook()


class Parser():
    """Represents control parser."""

    def __init__(self, owner):
        self.__owner = owner

    @property
    def control(self):
        """Get owning control instance."""
        return self.__owner

    @property
    def c(self):
        """Shortcut for control."""
        return self.__owner

    def parse_dates(self):
        """."""
        days_back = self.control.config['days_back']
        date_from = self.parse_date_from()-dt.timedelta(days=days_back)
        date_to = self.parse_date_to()-dt.timedelta(days=days_back)
        return (date_from, date_to)

    def parse_date_from(self):
        """Get data source date lower bound.

        Returns
        -------
        date_from : datetime or None
            Fetched records from data source should begin from this date.
        """
        timestamp = self.control.timestamp
        date = self._parse_date(timestamp, 0, 0, 0)
        return date

    def parse_date_to(self):
        """Get data source date upper bound.

        date_to : datetime or None
            Fetched records from data source should end with this date.
        """
        timestamp = self.control.timestamp
        date = self._parse_date(timestamp, 23, 59, 59)
        return date

    def parse_date(self, value, hour=None, minute=None, second=None):
        """Get date from initial raw value."""
        return self._parse_date(value, hour=hour, minute=minute, second=second)

    def _parse_date(self, value, hour=None, minute=None, second=None):
        date = utils.to_date(value)
        if hour is not None or minute is not None or second is not None:
            kwargs = {'hour': hour, 'minute': minute, 'second': second}
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            date = date.replace(**kwargs)
        return date

    def parse_source_table(self):
        """Get data source table.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting data source table.
        """
        name = self.control.source_name
        table = self._parse_source_table(name)
        return table

    def parse_source_table_a(self):
        """Get data source A table.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting data source A table.
        """
        name = self.control.source_name_a
        table = self._parse_source_table(name)
        return table

    def parse_source_table_b(self):
        """Get data source B table.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting data source B table.
        """
        name = self.control.source_name_b
        table = self._parse_source_table(name)
        return table

    def _parse_source_table(self, name):
        logger.debug(f'{self.c} Parsing source table {name}...')
        if isinstance(name, str) is True or name is None:
            table = db.table(name) if isinstance(name, str) is True else None
            if table is None:
                message = f'Source table {name} is not defined'
                raise AttributeError(message)
            else:
                logger.debug(f'{self.c} Source table {name} parsed')
                return table
        else:
            type = name.__class__.__name__
            message = f'source name must be str or None not {type}'
            raise TypeError(message)

    def parse_select(self):
        """Get SQL statement to fetch data from data source.

        Returns
        -------
        select : sqlalchemy.Select
        """
        table = self.control.source_table
        date_field = self.control.source_date_field
        select = self._parse_select(table, date_field=date_field)
        return select

    def parse_select_a(self):
        """Get SQL statement to fetch data from data source A.

        Returns
        -------
        select : sqlalchemy.Select
        """
        table = self.control.source_table_a
        date_field = self.control.source_date_field_a
        select = self._parse_select(table, date_field=date_field)
        return select

    def parse_select_b(self):
        """Get SQL statement to fetch data from data source B.

        Returns
        -------
        select : sqlalchemy.Select
        """
        table = self.control.source_table_b
        date_field = self.control.source_date_field_b
        select = self._parse_select(table, date_field=date_field)
        return select

    def _parse_select(self, table, date_field=None):
        logger.debug(f'{self.c} Parsing {table} select...')
        select = table.select()
        if isinstance(date_field, str) is True:
            column = table.c[date_field]

            datefmt = '%Y-%m-%d %H:%M:%S'
            date_from = self.control.date_from.strftime(datefmt)
            date_to = self.control.date_to.strftime(datefmt)

            datefmt = 'YYYY-MM-DD HH24:MI:SS'
            date_from = sa.func.to_date(date_from, datefmt)
            date_to = sa.func.to_date(date_to, datefmt)

            select = select.where(column.between(date_from, date_to))
            logger.debug(f'{self.c} {table} select parsed')
        return select

    def parse_output_names(self):
        """Get list with necessary output table names.

        Returns
        -------
        names : list
            List necessary output names.
        """
        names = []
        if (
            self.control.type == 'ANL'
            or (
                self.control.type == 'REC'
                and self.control.subtype == 'MA'
            )
            or self.control.type == 'REP'
        ):
            name = f'rapo_rest_{self.control.name}'.lower()
            names.append(name)
        return names

    def parse_output_tables(self):
        """Get list with existing output tables.

        Returns
        -------
        tables : list
            List of sqlalchemy.Table objects.
        """
        tables = []
        for name in self.control.output_names:
            if db.engine.has_table(name):
                table = db.table(name)
                tables.append(table)
        return tables

    def parse_temp_names(self):
        """Get list with necessary temporary table names.

        Returns
        -------
        names : list
            List necessary temporary names.
        """
        names = []
        fd = f'rapo_temp_fd_{self.control.process_id}'
        fda = f'rapo_temp_fda_{self.control.process_id}'
        fdb = f'rapo_temp_fdb_{self.control.process_id}'
        err = f'rapo_temp_err_{self.control.process_id}'
        md = f'rapo_temp_md_{self.control.process_id}'
        nmd = f'rapo_temp_nmd_{self.control.process_id}'
        if self.control.type == 'ANL':
            names.extend([fd, err])
        elif self.control.type == 'REP':
            names.append(fd)
        elif self.control.type == 'REC' and self.control.subtype == 'MA':
            names.extend([fda, fdb, md, nmd])
        return names

    def parse_temp_tables(self):
        """Get list with existing temporary tables.

        Returns
        -------
        tables : list
            List of sqlalchemy.Table objects.
        """
        tables = []
        for name in self.control.temp_names:
            if db.engine.has_table(name):
                table = db.table(name)
                tables.append(table)
        return tables

    def parse_analyze_error_config(self):
        """Get analyze error configuration.

        Returns
        -------
        config : list
            List with dictionaries where presented all keys for discrapancies.
        """
        logger.debug(f'{self.c} Parsing error configuration...')
        raw = self.control.config['error_config']
        config = []
        for item in json.loads(raw or '[]'):
            connexion = item.get('connexion', 'and').upper()
            column = item.get('column', '').lower()
            column_a = item.get('column_a', '').lower()
            column_b = item.get('column_b', '').lower()
            relation = item.get('relation', '<>').upper()
            value = item.get('value')
            is_column = item.get('is_column', False)

            config.append(
                {'connexion': connexion,
                 'column': column or None,
                 'column_a': column_a or None,
                 'column_b': column_b or None,
                 'relation': relation,
                 'value': value.lower() if is_column is True else value,
                 'is_column': is_column})
        logger.debug(f'{self.c} Error configuration parsed')
        return config

    def parse_reconciliation_rule_config(self):
        """Get control rule configuration.

        Returns
        -------
        config : list
            List with dictionaries where presented all keys for rule.
        """
        logger.debug(f'{self.c} Parsing rule configuration...')
        raw = self.control.config['rule_config']
        config = []
        for item in json.loads(raw or '[]'):
            column_a = item['column_a'].lower()
            column_b = item['column_b'].lower()

            new = {'column_a': column_a, 'column_b': column_b}
            config.append(new)
        logger.debug(f'{self.c} Rule configuration parsed')
        return config

    def parse_reconciliation_error_config(self):
        """Get reconciliation error configuration.

        Returns
        -------
        config : list
            List with dictionaries where presented all keys for mismatching.
        """
        logger.debug(f'{self.c} Parsing error configuration...')
        raw = self.control.config['error_config']
        config = []
        for item in json.loads(raw or '[]'):
            column_a = item['column_a'].lower()
            column_b = item['column_b'].lower()

            new = {'column_a': column_a, 'column_b': column_b}
            config.append(new)
        logger.debug(f'{self.c} Error configuration parsed')
        return config

    def parse_output_columns(self):
        """Get control output columns.

        Returns
        -------
        columns : list or None
            List with dictionaries where output columns configuration is
            presented.
            Will be None if configuration is not filled in RAPO_CONFIG.
        """
        config = self.control.config['output_table']
        columns = self._parse_output_columns(config)
        return columns

    def parse_output_columns_a(self):
        """Get control output columns A.

        Returns
        -------
        columns : list or None
            List with dictionaries where output A columns configuration is
            presented.
            Will be None if configuration is not filled in RAPO_CONFIG.
        """
        config = self.control.config['output_table_a']
        columns = self._parse_output_columns(config)
        return columns

    def parse_output_columns_b(self):
        """Get control output columns B.

        Returns
        -------
        columns : list or None
            List with dictionaries where output B columns configuration is
            presented.
            Will be None if configuration is not filled in RAPO_CONFIG.
        """
        config = self.control.config['output_table_b']
        columns = self._parse_output_columns(config)
        return columns

    def _parse_output_columns(self, config):
        logger.debug(f'{self.c} Parsing output columns...')
        config = json.loads(config) if isinstance(config, str) else None
        if config is None:
            logger.debug(f'{self.c} Output was not configured')
            return None
        else:
            column = dict.fromkeys(['column', 'column_a', 'column_b'])
            columns = []
            for value in config.get('columns', []):
                new = column.copy()
                if isinstance(value, str) is True:
                    new['column'] = value.lower()
                if isinstance(value, dict) is True:
                    for key in new.keys():
                        raw = value.get(key)
                        if isinstance(raw, str):
                            new[key] = raw.lower()
                columns.append(new)
            if columns:
                logger.debug(f'{self.c} Output columns parsed')
                return columns
            else:
                logger.debug(f'{self.c} Output columns was not configured')
                return None

    def parse_outdated_results(self):
        """Create list with tables and IDs of outdated control results."""
        log = db.tables.log
        control_id = self.control.id
        days_retention = self.control.config['days_retention']
        for table in self.parse_output_tables():
            select = sa.select([log.c.process_id])
            date = sa.func.trunc(sa.func.sysdate())-days_retention
            subq = sa.select([table.c.rapo_process_id])
            query = (select.where(log.c.control_id == control_id)
                           .where(log.c.added < date)
                           .where(log.c.process_id.in_(subq))
                           .order_by(log.c.process_id))
            text = db.formatter(query)
            logger.debug(f'{self.c} Searching outdated results in {table} '
                         f'with query:\n{text}')
            result = db.execute(query)
            pids = [row[0] for row in result]
            logger.debug(f'{self.c} Outdated results in {table}: {pids}')
            if pids:
                yield (table, pids)


class Executor():
    """Represents control executor."""

    def __init__(self, bind):
        self.__bind = bind

    @property
    def control(self):
        """Get binded control instance."""
        return self.__bind

    @property
    def c(self):
        """Get binded control instance. Same as control property."""
        return self.__bind

    def fetch_records(self):
        """Fetch data source.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with fetched data in case if DB is chosen
            engine.
        """
        select = self.control.select
        if self.control.engine == 'DB':
            tablename = f'rapo_temp_fd_{self.control.process_id}'
            table = self._fetch_records_to_table(select, tablename)
            return table

    def fetch_records_a(self):
        """Fetch data source A.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with fetched data in case if DB is chosen
            engine.
        """
        select = self.control.select_a
        if self.control.engine == 'DB':
            tablename = f'rapo_temp_fda_{self.control.process_id}'
            table = self._fetch_records_to_table(select, tablename)
            return table

    def fetch_records_b(self):
        """Fetch data source B.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with fetched data in case if DB is chosen
            engine.
        """
        select = self.control.select_b
        if self.control.engine == 'DB':
            tablename = f'rapo_temp_fdb_{self.control.process_id}'
            table = self._fetch_records_to_table(select, tablename)
            return table

    def analyze(self):
        """Run data analyze used for control with ANL type.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with found discrepancies in case if DB is
            chosen engine.
        """
        logger.debug(f'{self.c} Analyzing...')
        input_table = self.control.input_table
        output_columns = self.control.output_columns
        if output_columns is None or len(output_columns) == 0:
            select = input_table.select()
        else:
            columns = []
            for output_column in output_columns:
                name = output_column['column']
                column = input_table.c[name]
                columns.append(column)
            select = sa.select(columns)
        texts = []
        config = self.control.error_config
        for item in config:
            text = []
            connexion = item['connexion']
            if len(texts) > 0:
                text.append(connexion)
            column = str(input_table.c[item['column']])
            text.append(column)
            relation = item['relation']
            text.append(relation)
            value = item['value']
            is_column = item['is_column']
            value = str(input_table.c[value]) if is_column is True else value
            text.append(value)
            text = ' '.join(text)
            texts.append(text)
        select = select.where(sa.text('\n'.join(texts)))

        tablename = f'rapo_temp_err_{self.control.process_id}'
        select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
        ctas = sa.text(f'CREATE TABLE {tablename} AS\n{select}')
        text = db.formatter(ctas)
        logger.info(f'{self.c} Creating {tablename} with query:\n{text}')
        db.execute(ctas)
        logger.debug(f'{self.c} {tablename} created')

        table = db.table(tablename)
        logger.debug(f'{self.c} Analyzing done')
        return table

    def match(self):
        """Run data matching for control with REC type and MA subtype.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with matched data in case if DB is chosen
            engine.
        """
        logger.debug(f'{self.c} Defining matches...')

        table_a = self.control.input_table_a
        table_b = self.control.input_table_b

        columns = []
        output_columns = self.control.output_columns
        if output_columns is None or len(output_columns) == 0:
            columns.extend(table_a.columns)
            columns.extend(table_b.columns)
        else:
            for output_column in output_columns:
                name = output_column['column']

                column_a = output_column['column_a']
                column_a = table_a.c[column_a] if column_a else None

                column_b = output_column['column_b']
                column_b = table_b.c[column_b] if column_b else None

                if column_a is not None and column_b is not None:
                    column = sa.func.coalesce(column_a, column_b)
                elif column_a is not None:
                    column = column_a
                elif column_b is not None:
                    column = column_b

                column = column.label(name) if name else column
                columns.append(column)

        keys = []
        for rule in self.control.rule_config:
            column_a = table_a.c[rule['column_a']]
            column_b = table_b.c[rule['column_b']]
            keys.append(column_a == column_b)
        join = table_a.join(table_b, *keys)
        select = sa.select(columns).select_from(join)

        keys = []
        for error in self.control.error_config:
            column_a = table_a.c[error['column_a']]
            column_b = table_b.c[error['column_b']]
            select = select.where(column_a == column_b)

        tablename = f'rapo_temp_md_{self.control.process_id}'
        select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
        ctas = sa.text(f'CREATE TABLE {tablename} AS\n{select}')
        text = db.formatter(ctas)
        logger.info(f'{self.c} Creating {tablename} with query:\n{text}')
        db.execute(ctas)
        logger.debug(f'{self.c} {tablename} created')

        table = db.table(tablename)
        logger.debug(f'{self.c} Matches defined')
        return table

    def mismatch(self):
        """Run data mismatching for control with REC type and MA subtype.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with mismatched data in case if DB is
            chosen engine.
        """
        logger.debug(f'{self.c} Defining mismatches...')

        table_a = self.control.input_table_a
        table_b = self.control.input_table_b

        columns = []
        output_columns = self.control.output_columns
        if output_columns is None or len(output_columns) == 0:
            columns.extend(table_a.columns)
            columns.extend(table_b.columns)
        else:
            for output_column in output_columns:
                name = output_column['column']

                column_a = output_column['column_a']
                column_a = table_a.c[column_a] if column_a else None

                column_b = output_column['column_b']
                column_b = table_b.c[column_b] if column_b else None

                if column_a is not None and column_b is not None:
                    column = sa.func.coalesce(column_a, column_b)
                elif column_a is not None:
                    column = column_a
                elif column_b is not None:
                    column = column_b

                column = column.label(name) if name else column
                columns.append(column)

        keys = []
        for rule in self.control.rule_config:
            column_a = table_a.c[rule['column_a']]
            column_b = table_b.c[rule['column_b']]
            keys.append(column_a == column_b)
        join = table_a.join(table_b, *keys)
        select = sa.select(columns).select_from(join)

        keys = []
        for error in self.control.error_config:
            column_a = table_a.c[error['column_a']]
            column_b = table_b.c[error['column_b']]
            select = select.where(column_a != column_b)

        tablename = f'rapo_temp_nmd_{self.control.process_id}'
        select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
        ctas = sa.text(f'CREATE TABLE {tablename} AS\n{select}')
        text = db.formatter(ctas)
        logger.info(f'{self.c} Creating {tablename} with query:\n{text}')
        db.execute(ctas)
        logger.debug(f'{self.c} {tablename} created')

        table = db.table(tablename)
        logger.debug(f'{self.c} Mismatches defined')
        return table

    def count_fetched(self):
        """Count fetched records in data source."""
        if self.control.engine == 'DB':
            table = self.control.input_table
            fetched = self._count_fetched_to_table(table)
        return fetched

    def count_fetched_a(self):
        """Count fetched records in data source A."""
        if self.control.engine == 'DB':
            table = self.control.input_table_a
            fetched_a = self._count_fetched_to_table(table)
        return fetched_a

    def count_fetched_b(self):
        """Count fetched records in data source B."""
        if self.control.engine == 'DB':
            table = self.control.input_table_b
            fetched_b = self._count_fetched_to_table(table)
        return fetched_b

    def count_errors(self):
        """Count found discrepancies for control with ANL type.

        Returns
        -------
        errors : int
            Number of found discrepancies.
        """
        logger.debug(f'{self.c} Counting errors...')
        if self.control.engine == 'DB':
            table = self.control.error_table
            count = sa.select([sa.func.count()]).select_from(table)
            errors = db.execute(count).scalar()
        logger.debug(f'{self.c} Errors counted')
        return errors

    def count_matched(self):
        """Count records that were matched.

        Returns
        -------
        matched : int
            Number of found discrepancies.
        """
        logger.debug(f'{self.c} Counting matched...')
        if self.control.engine == 'DB':
            table = self.control.result_table
            count = sa.select([sa.func.count()]).select_from(table)
            matched = db.execute(count).scalar()
        logger.debug(f'{self.c} Matched counted')
        return matched

    def count_mismatched(self):
        """Count records that were not matched.

        Returns
        -------
        mismatched : int
            Number of found discrepancies.
        """
        logger.debug(f'{self.c} Counting mismatched')
        if self.control.engine == 'DB':
            table = self.control.error_table
            count = sa.select([sa.func.count()]).select_from(table)
            mismatched = db.execute(count).scalar()
        logger.debug(f'{self.c} Mismatched counted')
        return mismatched

    def save_results(self):
        """Save defined results as output records."""
        logger.debug(f'{self.c} Start saving...')
        table = self.control.result_table
        process_id = sa.literal(self.control.process_id)
        select = sa.select([*table.columns,
                            process_id.label('rapo_process_id')])
        table = self.prepare_output_table()
        insert = table.insert().from_select(table.columns, select)
        db.execute(insert)
        logger.debug(f'{self.c} Saving done')

    def save_errors(self):
        """Save defined errors as output records."""
        logger.debug(f'{self.c} Start saving...')
        table = self.control.error_table
        process_id = sa.literal(self.control.process_id)
        select = sa.select([*table.columns,
                            process_id.label('rapo_process_id')])
        table = self.prepare_output_table()
        insert = table.insert().from_select(table.columns, select)
        db.execute(insert)
        logger.debug(f'{self.c} Saving done')

    def save_matches(self):
        """Save found matches as RAPO results."""
        return self.save_results()

    def save_mismatches(self):
        """Save found mismatches as RAPO results."""
        return self.save_errors()

    def delete_output_records(self):
        """Delete records saved as control results in DB table."""
        for table in self.control.output_tables:
            id = table.c.rapo_process_id
            delete = table.delete().where(id == self.control.process_id)
            db.execute(delete)

    def prepare_output_table(self):
        """Check RAPO_RESULT and create it at initial control run.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting RAPO_RESULT.
        """
        tablename = f'rapo_rest_{self.control.name}'.lower()
        if db.engine.has_table(tablename) is False:
            logger.debug(f'{self.c} Table {tablename} will be created')

            columns = []
            output_columns = self.control.output_columns
            if output_columns is None or len(output_columns) == 0:
                if self.c.config['source_name'] is not None:
                    columns.extend(self.c.source_table.columns)
                if self.c.config['source_name_a'] is not None:
                    columns.extend(self.c.source_table_a.columns)
                if self.c.config['source_name_b'] is not None:
                    columns.extend(self.c.source_table_b.columns)
            else:
                for output_column in output_columns:
                    name = output_column['column']
                    column_a = output_column['column_a']
                    column_b = output_column['column_b']
                    if column_a is not None or column_b is not None:
                        table_a = self.control.source_table_a
                        table_b = self.control.source_table_b
                        if column_a is not None and column_b is not None:
                            column_a = table_a.c[column_a]
                            column_b = table_b.c[column_b]
                            column = sa.func.coalesce(column_a, column_b)
                        elif column_a is not None:
                            column = table_a.c[column_a]
                        elif column_b is not None:
                            column = table_b.c[column_b]
                        column = column.label(name) if name else column
                    else:
                        table = self.control.source_table
                        column = table.c[name]
                    columns.append(column)

            process_id = sa.literal(self.control.process_id)
            columns = [*columns, process_id.label('rapo_process_id')]
            select = sa.select(columns)
            select = select.where(sa.literal(1) == sa.literal(0))
            select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
            ctas = f'CREATE TABLE {tablename} AS\n{select}'
            index = (f'CREATE INDEX {tablename}_rapo_process_id_ix '
                     f'ON {tablename}(rapo_process_id) COMPRESS')
            compress = (f'ALTER TABLE {tablename} '
                        'MOVE ROW STORE COMPRESS ADVANCED')
            text = db.formatter(ctas, index, compress)
            logger.debug(f'{self.c} Creating table {tablename} '
                         f'with query:\n{text}')
            db.execute(ctas)
            db.execute(index)
            db.execute(compress)
            logger.debug(f'{self.c} {tablename} created')
        table = db.table(tablename)
        return table

    def drop_temporary_tables(self):
        """Clean all temporary tables created during control execution."""
        logger.debug(f'{self.c} Dropping temporary tables...')
        for table in self.control.temp_tables:
            table.drop(db.engine)
        logger.debug(f'{self.c} Temporary tables dropped')

    def prerun_hook(self):
        """Execute database prerun hook function."""
        logger.debug(f'{self.c} Executing prerun hook function...')
        process_id = self.control.process_id
        select = f'select rapo_prerun_control_hook({process_id}) from dual'
        stmt = sa.text(select)
        try:
            result_code = db.execute(stmt).scalar()
            if result_code is None or result_code.upper() == 'OK':
                logger.debug(f'{self.c} Hook function evaluated OK')
                return True, result_code
            else:
                logger.debug(f'{self.c} Hook function evaluated NOT OK '
                             f'[{result_code}]')
                return False, result_code
        except Exception:
            logger.error(f'{self.c} Error evaluating prerun hook')
            logger.error()

    def postrun_hook(self):
        """Execute database postrun hook procedure."""
        logger.debug(f'{self.c} Executing postrun hook procedure...')
        process_id = self.control.process_id
        stmt = sa.text(f'begin rapo_postrun_control_hook({process_id}); end;')
        db.execute(stmt)
        logger.debug(f'{self.c} Hook procedure executed')

    def _fetch_records_to_table(self, select, tablename):
        logger.debug(f'{self.c} Start fetching...')
        select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
        ctas = sa.text(f'CREATE TABLE {tablename} AS\n{select}')
        text = db.formatter(ctas)
        logger.info(f'{self.c} Creating {tablename} with query:\n{text}')
        db.execute(ctas)
        logger.info(f'{self.c} {tablename} created')
        table = db.table(tablename)
        logger.debug(f'{self.c} Fetching done')
        return table

    def _count_fetched_to_table(self, table):
        logger.debug(f'{self.c} Counting fetched in {table}...')
        count = sa.select([sa.func.count()]).select_from(table)
        fetched = db.execute(count).scalar()
        logger.debug(f'{self.c} Fetched in {table} counted')
        return fetched
