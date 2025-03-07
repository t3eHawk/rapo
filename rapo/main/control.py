"""Contains RAPO control interface."""

import sys
import threading as th
import multiprocessing as mp
import traceback as tb

import re
import json
import time as tm
import datetime as dt

import sqlalchemy as sa

from ..database import db
from ..logger import logger
from ..reader import reader
from ..utils import utils

from .fields import RESULT_KEY, RESULT_VALUE, RESULT_TYPE
from .case import NORMAL, INFO, ERROR, WARNING, INCIDENT, DISCREPANCY


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
                 iteration_id=None, date=None, date_from=None, date_to=None):
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

            self.iteration_id = None
            self.timestamp = None
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

            self.iteration_id = iteration_id
            if self.iteration_id:
                iteration_config = utils.get_config(self.iteration_id,
                                                    self.iteration_config)
                self.period_back = iteration_config['period_back']
                self.period_number = iteration_config['period_number']
                self.period_type = iteration_config['period_type']
            else:
                self.period_back = self.config['period_back']
                self.period_number = self.config['period_number']
                self.period_type = self.config['period_type']

            self.timestamp = timestamp
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
    def date(self):
        """Get control date if relevant."""
        if self.date_from.date() == self.date_to.date():
            return self.date_from.date()

    @property
    def process_id(self):
        """Get control run process ID."""
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
    def pid(self):
        """Shortcut for process_id."""
        return self._process_id

    @property
    def source_name(self):
        """Get control data source name."""
        return self.parser.parse_source_name()

    @property
    def source_filter(self):
        """Get control data source filter clause."""
        return self.parser.parse_filter()

    @property
    def source_date_field(self):
        """Get control data source date field."""
        return utils.to_lower(self.config['source_date_field'])

    @property
    def source_name_a(self):
        """Get control data source A name."""
        return self.parser.parse_source_name_a()

    @property
    def source_filter_a(self):
        """Get control data source A filter clause."""
        return self.parser.parse_filter_a()

    @property
    def source_date_field_a(self):
        """Get control data source A date field."""
        return utils.to_lower(self.config['source_date_field_a'])

    @property
    def source_name_b(self):
        """Get control data source B name."""
        return self.parser.parse_source_name_b()

    @property
    def source_filter_b(self):
        """Get control data source B filter clause."""
        return self.parser.parse_filter_b()

    @property
    def source_date_field_b(self):
        """Get control data source B date field."""
        return utils.to_lower(self.config['source_date_field_b'])

    @property
    def result_columns(self):
        """Get object representing result columns."""
        return self.parser.parse_result_columns()

    @property
    def key_column(self):
        """Get process identification column."""
        return self.parser.parse_key_column()

    @property
    def variables(self):
        """Get control variables."""
        return self.parser.parse_variables()

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
    def is_analysis(self):
        """Identify whether control is analysis or not."""
        return True if self.type == 'ANL' else False

    @property
    def is_reconciliation(self):
        """Identify whether control is reconciliation or not."""
        return True if self.type == 'REC' else False

    @property
    def is_report(self):
        """Identify whether control is report or not."""
        return True if self.type == 'ANL' else False

    @property
    def has_cases(self):
        """Identify whether control is case-configured or not."""
    @property
    def has_iterations(self):
        """Identify whether control is iteration-configured or not."""
        if self.config['iteration_config']:
            return True
        return False

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
    def case_config(self):
        """Get control case configuration."""
        return self.parser.parse_case_config()

    @property
    def result_config(self):
        """Get control result configuration."""
        return self.parser.parse_result_config()

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
    def error_sql(self):
        """Get control error SQL expression."""
        if self.type == 'ANL':
            return self.parser.parse_analyze_error_sql()

    @property
    def iteration_config(self):
        """Get control iteration configuration."""
        return self.parser.parse_iteration_config()

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
    def mandatory_columns(self):
        """Get control mandatory output columns configuration."""
        return self.parser.parse_mandatory_columns()

    @property
    def parallelism(self):
        """Get parameter describing execution parallelism."""
        return self.config['parallelism']

    @property
    def need_a(self):
        """Get parameter defining necessity of data source A saving."""
        return self.parser.parse_boolean('need_a')

    @property
    def need_b(self):
        """Get parameter defining necessity of data source B saving."""
        return self.parser.parse_boolean('need_b')

    @property
    def with_deletion(self):
        """Get parameter to clear output table before usage."""
        return self.parser.parse_boolean('with_deletion')

    @property
    def with_drop(self):
        """Get parameter to drop output table before usage."""
        return self.parser.parse_boolean('with_drop')

    @property
    def need_hook(self):
        """Get parameter defining necessity of hook execution."""
        return self.parser.parse_boolean('need_hook')

    @property
    def need_prerun_hook(self):
        """Get parameter defining necessity of prerun hook execution."""
        return self.parser.parse_boolean('need_prerun_hook')

    @property
    def need_postrun_hook(self):
        """Get parameter defining necessity of postrun hook execution."""
        return self.parser.parse_boolean('need_postrun_hook')

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

    def iterate(self):
        """Run all additional control iterations."""
        for case in self.iteration_config:
            iteration_id = case['iteration_id']
            iteration_status = case['status']
            if iteration_status:
                logger.info(f'{self} Iterating control '
                            f'using configuration {case}')
                control = self.__class__(name=self.name,
                                         timestamp=self.timestamp,
                                         iteration_id=iteration_id)
                control.run()

    def prerequisite(self):
        """Get the result of the prerequisite statement."""
        return self._prerequisite()

    def prepare(self):
        """Execute control preparation SQL scripts."""
        self._prepare()

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

    def _prerequisite(self):
        statement = self.parser.parse_prerequisite_statement()
        if statement:
            logger.info(f'{self} Checking control prerequisite statement...')
            try:
                result = db.execute(statement).scalar()
                self.prerequisite_value = result
                logger.info(f'{self} Control prerequisite statement '
                            f'returns {self.prerequisite_value}')
                self._update(prerequisite_value=self.prerequisite_value)
                if not self.prerequisite_value:
                    return False
            except Exception:
                logger.error()
                return self._escape()
            else:
                return self._continue()
        return self._continue()

    def _prepare(self):
        statement = self.parser.parse_preparation_statement()
        if statement:
            logger.info(f'{self} Running control preparation SQL scripts...')
            try:
                result = db.execute(statement)
                rowcount = result.rowcount
                logger.info(f'{self} Control preparation SQL scripts '
                            f'successfully performed returning {rowcount}')
            except Exception:
                logger.error()
                return self._escape()
            else:
                return self._continue()
        return self._continue()

    def _resume(self):
        if self.initiated:
            if self._prepare():
                if self._prerequisite():
                    if self._prerun_hook():
                        if self._start():
                            if self._progress():
                                if self._finish():
                                    if self._complete():
                                    if self._done():
                                        self._postrun_hook()
                else:
                    self._do_not_resume()
            else:
                self._can_not_prepare()

    def _do_not_resume(self):
        if not self.prerequisite_value:
            logger.info(f'{self} Control will not be resumed '
                        'due to a prerequisite check')
            message = ('Control execution stopped because the '
                       'PREREQUISITE check not passed')
            self._update(text_message=message)

    def _can_not_prepare(self):
        logger.info(f'{self} Control will not be resumed '
                    'due to a preparation failure')
        message = ('Control execution stopped because the PREPARATION failed')
        self._update(text_message=message)

    def _spawn(self):
        logger.debug(f'{self} Spawning new process for the control...')
        context = mp.get_context('spawn')
        self.process = context.Process(name=self.name, target=self._resume)
        self.process.start()
        self.handler = th.Thread(name=f'{self}-Handler', target=self._handle)
        self.handler.start()
        logger.info(f'{self} Running as process on PID {self.process.pid}')

    def _handle(self):
        process = self.process
        while process.is_alive():
            control = Control(process_id=self.process_id)
            if not control.status:
                logger.info(f'{self} Control cancelation request received')
                self.process = process
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
        process = self.process
        pid = process.pid
        logger.info(f'{self} Terminating process at PID {pid}...')
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

    def _complete(self):
        statement = self.parser.parse_completion_statement()
        if statement:
            logger.info(f'{self} Running control completion SQL scripts...')
            try:
                result = db.execute(statement)
                rowcount = result.rowcount
                logger.info(f'{self} Control completion SQL scripts '
                            f'successfully performed returning {rowcount}')
            except Exception:
                logger.error()
                return self._escape()
            else:
                return self._continue()
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
        days_retention = self.config['days_retention']
        if days_retention == 0:
            for table in self.output_tables:
                repr = f'[{self.name}]'
                logger.info(f'{repr} Deleting all results in {table}...')
                db.truncate(table.name)
                logger.info(f'{repr} Results in {table} deleted')
        else:
            outdated_results = list(self.parser.parse_outdated_results())
            if outdated_results:
                for table, process_ids in outdated_results:
                    for process_id in process_ids:
                        repr = f'[{self.name}:{process_id}]'
                        logger.info(f'{repr} Deleting results in {table}...')
                        id = table.c.rapo_process_id
                        query = table.delete().where(id == process_id)
                        text = db.formatter.document(query)
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

    def parse_boolean(self, name):
        """Prepare a boolean value of the parameter by name.

        Parameters
        ----------
        name : str
            Name of the parameter from the configuration table.

        Returns
        -------
        value : bool
            Parameter value represented as boolean.
        """
        return True if self.control.config[name] == 'Y' else False

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

    def parse_dates(self):
        """Parse control dates according to configuration."""
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
        period_back = self.control.period_back
        period_type = self.control.period_type
        current_date = self._parse_date(timestamp, 0, 0, 0)
        if period_type == 'D':
            target_date = current_date-dt.timedelta(days=period_back)
        elif period_type == 'M':
            calculated_date = utils.get_month_date_from(current_date)
            while period_back:
                calculated_date = calculated_date-dt.timedelta(days=1)
                calculated_date = utils.get_month_date_from(calculated_date)
                period_back -= 1
            target_date = calculated_date.replace()
        elif period_type == 'W':
            target_date = current_date-dt.timedelta(weeks=period_back)
        return target_date

    def parse_date_to(self):
        """Get data source date upper bound.

        date_to : datetime or None
            Fetched records from data source should end with this date.
        """
        period_number = self.control.period_number
        period_type = self.control.period_type
        current_date = self._parse_date(self.parse_date_from(), 23, 59, 59)
        if period_type == 'D':
            target_date = current_date+dt.timedelta(days=period_number-1)
        elif period_type == 'M':
            calculated_date = utils.get_month_date_to(current_date)
            while period_number-1:
                calculated_date = calculated_date+dt.timedelta(days=1)
                calculated_date = utils.get_month_date_to(calculated_date)
                period_number -= 1
            target_date = calculated_date.replace()
        elif period_type == 'W':
            calculated_date = current_date+dt.timedelta(weeks=period_number)
            target_date = calculated_date-dt.timedelta(days=1)
        return target_date

    def parse_source_name(self):
        """Get data source name."""
        return self._parse_source_name('source_name')

    def parse_source_name_a(self):
        """Get data source A name."""
        return self._parse_source_name('source_name_a')

    def parse_source_name_b(self):
        """Get data source B name."""
        return self._parse_source_name('source_name_b')

    def _parse_source_name(self, source_name):
        custom_name = self.control.config[source_name]
        if custom_name:
            custom_name = custom_name.format(**self.c.variables)
            final_name = utils.to_lower(custom_name)
            return final_name

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
        literals = self.control.result_columns
        where = self.control.source_filter
        date_field = self.control.source_date_field
        select = self._parse_select(table, literals=literals, where=where,
                                    date_field=date_field)
        return select

    def parse_select_a(self):
        """Get SQL statement to fetch data from data source A.

        Returns
        -------
        select : sqlalchemy.Select
        """
        table = self.control.source_table_a
        where = self.control.source_filter_a
        date_field = self.control.source_date_field_a
        select = self._parse_select(table, where=where, date_field=date_field)
        return select

    def parse_select_b(self):
        """Get SQL statement to fetch data from data source B.

        Returns
        -------
        select : sqlalchemy.Select
        """
        table = self.control.source_table_b
        where = self.control.source_filter_b
        date_field = self.control.source_date_field_b
        select = self._parse_select(table, where=where, date_field=date_field)
        return select

    def _parse_select(self, table, literals=None, where=None, date_field=None):
        logger.debug(f'{self.c} Parsing {table} select...')
        literals = literals if isinstance(literals, list) else []
        select = sa.select([*table.columns, *literals])
        if isinstance(where, str):
            clause = sa.text(where)
            select = select.where(clause)
        if isinstance(date_field, str):
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

    def parse_filter(self):
        """Get SQL-like expression used to filter the data source.
        """
        return self._parse_filter('source_filter')

    def parse_filter_a(self):
        """Get SQL-like expression used to filter the data source A.
        """
        return self._parse_filter('source_filter_a')

    def parse_filter_b(self):
        """Get SQL-like expression used to filter the data source B.
        """
        return self._parse_filter('source_filter_b')

    def _parse_filter(self, filter_name):
        return self.control.config[filter_name]

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

    def parse_prerequisite_statement(self):
        """Prepare prerequisite statement taken from the configuration table.

        Returns
        -------
        final_statement : str or None
            Formatted SQL query representing prerequisite statement.
        """
        return self._parse_statement('prerequisite_sql')

    def parse_preparation_statement(self):
        """Get preparation statement taken from the configuration table.

        Returns
        -------
        final_statement : str or None
            Formatted SQL query that must be performed before the control.
        """
        return self._parse_statement('preparation_sql')

    def parse_completion_statement(self):
        """Get completion statement taken from the configuration table.

        Returns
        -------
        final_statement : str or None
            Formatted SQL query that must be performed after the control.
        """
        return self._parse_statement('completion_sql')

    def _parse_statement(self, statement_name):
        custom_statement = self.control.config[statement_name]
        if custom_statement:
            custom_statement = custom_statement.format(**self.c.variables)
            final_statement = db.formatter(custom_statement)
            return final_statement

    def parse_case_config(self):
        """Get case mapping taken from the configuration table.

        Returns
        -------
        config : dict or None
            Dictionary with case mapping.
        """
        logger.debug(f'{self.c} Parsing case configuration...')
        string = self.control.config['case_config']
        if string:
            case_list = [NORMAL, INFO, ERROR, WARNING, INCIDENT, DISCREPANCY]
            custom_config = json.loads(string)
            final_config = {}
            for custom_record in custom_config:
                i = custom_record['case_id']
                case_id = custom_record['case_id']
                case_value = custom_record['case_value']
                case_type = custom_record.get('case_type')
                case_description = custom_record.get('case_description')
                final_record = {
                    'case_id': case_id,
                    'case_value': case_value,
                    'case_type': case_type if case_type in case_list else None,
                    'case_description': case_description
                }
                final_config[i] = final_record
            logger.debug(f'{self.c} Case configuration parsed')
            return final_config
        else:
            logger.debug(f'{self.c} Case configuration not found')

    def parse_result_config(self):
        """Get main result statement taken from the configuration table.

        Returns
        -------
        config : str or None
            SQL-like logical expression with a result mapping.
        """
        logger.debug(f'{self.c} Parsing result configuration...')
        string = self.control.config['result_config']
        if string:
            custom_config = self.control.config['result_config']
            final_config = db.formatter(custom_config)
            logger.debug(f'{self.c} Result configuration parsed')
            return final_config
        else:
            logger.debug(f'{self.c} Result configuration not found')

    def parse_analyze_error_config(self):
        """Get analyze error configuration.

        Returns
        -------
        config : list or None
            List with dictionaries where presented all keys for discrapancies.
        """
        logger.debug(f'{self.c} Parsing error configuration...')
        string = self.control.config['error_config']
        if utils.is_json(string):
            config = self._parse_json_filter(string)
            logger.debug(f'{self.c} Error configuration parsed')
            return config
        else:
            logger.debug(f'{self.c} Error configuration not found')

    def parse_analyze_error_sql(self):
        """Prepare analyze error SQL expression.

        Returns
        -------
        expression : str or None
            String with SQL expression to select discrapancies.
        """
        logger.debug(f'{self.c} Parsing error SQL...')
        string = self.control.config['error_config']
        if utils.is_json(string):
            table = self.control.input_table
            expressions = []
            config = self.parse_analyze_error_config()
            for i in config:
                expression = []
                connexion = i['connexion']
                if len(expressions) > 0:
                    expression.append(connexion)
                column = str(table.c[i['column']])
                expression.append(column)
                relation = i['relation']
                expression.append(relation)
                value = i['value']
                is_column = i['is_column']
                value = str(table.c[value]) if is_column else value
                expression.append(value)
                expression = ' '.join(expression)
                expressions.append(expression)
            expression = '\n'.join(expressions)
            logger.debug(f'{self.c} Error SQL parsed using configuration')
            return expression
        elif utils.is_sql(string):
            expression = self._parse_sql_filter(string)
            logger.debug(f'{self.c} Error SQL parsed')
            return expression
        elif not string and self.control.has_cases:
            table = self.control.input_table
            result_type = table.c.rapo_result_type
            target_types = [INFO, ERROR, WARNING, INCIDENT, DISCREPANCY]
            clause = sa.or_(result_type.in_(target_types),
                            result_type.is_(None))
            statement = db.compile(clause)
            expression = statement.string
            return expression
        else:
            logger.debug(f'{self.c} Error SQL not found')
            return ''

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

    def _parse_json_filter(self, string):
        config = []
        for item in json.loads(string or '[]'):
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
        return config

    def _parse_sql_filter(self, string):
        return string

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

    def parse_mandatory_columns(self):
        """Get control mandatory columns."""
        logger.debug(f'{self.c} Parsing mandatory columns...')
        if self.control.is_analysis:
            columns = [RESULT_KEY, RESULT_VALUE, RESULT_TYPE]
            logger.debug(f'{self.c} Mandatory columns parsed')
            return columns
        else:
            logger.debug(f'{self.c} Mandatory columns not defined')

    def parse_result_columns(self):
        """Get result columns based on result statement and case configuration.

        Returns
        -------
        columns : List of sqlalchemy columns or None
            Object representing result column.
        """
        logger.debug(f'{self.c} Parsing result columns...')
        custom_statement = self.control.config['result_config']
        if self.control.is_analysis and custom_statement:
            columns = []
            case_config = self.parse_case_config()

            replaces = []
            pattern = r'THEN\s+\d+|ELSE\s+\d+'
            matches = re.findall(pattern, custom_statement, re.IGNORECASE)
            for match in matches:
                keyword, result = re.split(r'\s+', match)
                case_id = int(result)
                case_value = case_config[case_id]['case_value']
                case_type = case_config[case_id]['case_type']

                replace = [match, {}]
                replace[1]['key'] = f'{keyword} {case_id}'
                replace[1]['value'] = f'{keyword} \'{case_value}\''
                replace[1]['type'] = f'{keyword} \'{case_type}\''
                replaces.append(replace)

            columns = []
            for field in ['key', 'value', 'type']:
                field_name = f'rapo_result_{field}'
                final_statement = custom_statement
                for replace in replaces:
                    old = replace[0]+r'\s'
                    new = replace[1][field]+r'\n'
                    final_statement = re.sub(old, new, final_statement)
                final_statement = db.formatter(final_statement)
                column = sa.literal_column(final_statement).label(field_name)
                columns.append(column)
            logger.debug(f'{self.c} Result columns parsed')
            return columns
        elif self.control.is_analysis and not custom_statement:
            columns = []
            fields = [RESULT_KEY, RESULT_VALUE, RESULT_TYPE]
            for field in fields:
                column = field.null
                columns.append(column)
            logger.debug(f'{self.c} Result columns not configured')
            return columns
        else:
            logger.debug(f'{self.c} Result columns not parsed')

    def parse_key_column(self):
        """Get process identification and description column.

        Returns
        -------
        columns : sqlalchemy column
            Object representing process identification column.
        """
        column = sa.literal(self.control.process_id).label('rapo_process_id')
        return column

    def parse_iteration_config(self):
        """Get appropriate period parameters for this control iteration."""
        input_string = self.control.config['iteration_config'] or '[]'
        input_config = json.loads(input_string)
        output_config = []
        for item_config in input_config:
            iteration_id = item_config['iteration_id']
            iteration_description = item_config.get('iteration_description')
            period_back = item_config['period_back']
            period_number = item_config['period_number']
            period_type = item_config['period_type']
            status = True if item_config['status'] == 'Y' else False

            add_config = {
                'iteration_id': iteration_id,
                'iteration_description': iteration_description,
                'period_back': period_back,
                'period_number': period_number,
                'period_type': period_type,
                'status': status
            }
            output_config.append(add_config)
        return output_config

    def parse_parallelism_hint(self):
        """Get SQL expression with parallelism hint.

        Returns
        -------
        expression : string
            String with SQL expression with parallelism hint.
        """
        degree_of_parallelism = self.control.parallelism
        if degree_of_parallelism:
            expression = f'/*+ parallel({degree_of_parallelism}) */'
        else:
            expression = ''
        return expression

    def parse_variables(self):
        """Get control variables.

        Returns
        -------
        variables : dict
            Dictionary with control variables.
        """
        control = self.control
        variables = dict(control_name=control.name,
                         control_date=control.date,
                         control_date_from=control.date_from,
                         control_date_to=control.date_to,
                         process_id=control.process_id)
        return variables

    def parse_outdated_results(self):
        """Create list with tables and IDs of outdated control results."""
        log = db.tables.log
        control_id = self.control.id
        days_retention = self.control.config['days_retention']
        today = dt.date.today().strftime(r'%Y-%m-%d')
        current_date = sa.func.to_date(today, 'YYYY-MM-DD')
        target_date = current_date-days_retention
        for table in self.parse_output_tables():
            select = sa.select([log.c.process_id])
            subq = sa.select([table.c.rapo_process_id])
            query = (select.where(log.c.control_id == control_id)
                           .where(log.c.added < target_date)
                           .where(log.c.process_id.in_(subq))
                           .order_by(log.c.process_id))
            text = db.formatter.document(query)
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
        mandatory_columns = self.control.mandatory_columns
        if output_columns is None or len(output_columns) == 0:
            select = input_table.select()
        else:
            columns = []
            for output_column in output_columns:
                name = output_column['column']
                column = input_table.c[name]
                columns.append(column)
            for mandatory_column in mandatory_columns:
                name = mandatory_column.column_name
                column = input_table.c[name]
                columns.append(column)
            select = sa.select(columns)

        tablename = f'rapo_temp_err_{self.control.process_id}'
        clause = sa.text(self.control.error_sql)
        select = select.where(clause)
        select = db.compile(select)
        ctas = sa.text(f'CREATE TABLE {tablename} AS\n{select}')
        text = db.formatter.document(ctas)
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
        select = db.compile(select)
        ctas = sa.text(f'CREATE TABLE {tablename} AS\n{select}')
        text = db.formatter.document(ctas)
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

        keys = None
        for rule in self.control.rule_config:
            column_a = table_a.c[rule['column_a']]
            column_b = table_b.c[rule['column_b']]
            if keys is None:
                keys = (column_a == column_b)
            else:
                keys &= (column_a == column_b)
        join = table_a.outerjoin(table_b, keys)
        select = sa.select(columns).select_from(join)

        for error in self.control.error_config:
            column_a = table_a.c[error['column_a']]
            column_b = table_b.c[error['column_b']]
            select = select.where((column_a != column_b) | (column_b == None) )

        tablename = f'rapo_temp_nmd_{self.control.process_id}'
        select = db.compile(select)
        ctas = sa.text(f'CREATE TABLE {tablename} AS\n{select}')
        text = db.formatter.document(ctas)
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
        process_id = self.control.key_column
        select = sa.select([*table.columns, process_id])
        table = self.prepare_output_table()
        insert = table.insert().from_select(table.columns, select)
        db.execute(insert)
        logger.debug(f'{self.c} Saving done')

    def save_errors(self):
        """Save defined errors as output records."""
        logger.debug(f'{self.c} Start saving...')
        table = self.control.error_table
        process_id = self.control.key_column
        select = sa.select([*table.columns, process_id])
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
            if self.control.with_deletion:
                db.truncate(table)
            else:
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
        if self.control.with_deletion or self.control.with_drop:
            if db.engine.has_table(tablename):
                if self.control.with_deletion:
                    db.truncate(tablename)
                elif self.control.with_drop:
                    db.drop(tablename)
        if not db.engine.has_table(tablename):
            logger.debug(f'{self.c} Table {tablename} will be created')

            columns = []
            output_columns = self.control.output_columns
            mandatory_columns = self.control.mandatory_columns
            if not output_columns or len(output_columns) == 0:
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
            if mandatory_columns:
                for mandatory_column in mandatory_columns:
                    column = mandatory_column.null
                    columns.append(column)

            process_id = self.control.key_column
            columns = [*columns, process_id]
            select = sa.select(columns)
            select = select.where(sa.literal(1) == sa.literal(0))
            select = db.compile(select)
            ctas = f'CREATE TABLE {tablename} AS\n{select}'
            index = (f'CREATE INDEX {tablename}_rapo_process_id_ix '
                     f'ON {tablename}(rapo_process_id) COMPRESS')
            compress = (f'ALTER TABLE {tablename} '
                        'MOVE ROW STORE COMPRESS ADVANCED')
            text = db.formatter.document(ctas, index, compress)
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
            db.purge(table.name)
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

    def _fetch_records_to_table(self, select, table_name):
        logger.debug(f'{self.c} Start fetching...')
        parallelism = self.control.config['parallelism']
        if parallelism:
            hint = f'parallel({parallelism})'
            select = select.with_hint(sa.text(table_name), hint)
        select = db.compile(select)
        ctas = sa.text(f'CREATE TABLE {table_name} AS\n{select}')
        text = db.formatter.document(ctas)
        logger.info(f'{self.c} Creating {table_name} with query:\n{text}')
        db.execute(ctas)
        logger.info(f'{self.c} {table_name} created')
        table = db.table(table_name)
        logger.debug(f'{self.c} Fetching done')
        return table

    def _count_fetched_to_table(self, table):
        logger.debug(f'{self.c} Counting fetched in {table}...')
        count = sa.select([sa.func.count()]).select_from(table)
        fetched = db.execute(count).scalar()
        logger.debug(f'{self.c} Fetched in {table} counted')
        return fetched

    def _index_table(self, table_name, key_field_name):
        logger.debug(f'{self.c} Creating index for {table_name}...')
        index_name = f'{table_name}_ix'
        parallelism = self.control.config['parallelism'] or 1
        create_index = (f'create index {index_name} '
                        f'on {table_name} ({key_field_name}) unusable')
        rebuild_index = (f'alter index {index_name} rebuild online '
                         f'parallel {parallelism} '
                         'nologging')
        db.execute(create_index)
        db.execute(rebuild_index)
        logger.debug(f'{self.c} Index for {table_name} created')
