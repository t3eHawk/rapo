"""Contains database instance with application schema."""

import os
import sys
import threading as th
import queue as qe

import sqlalchemy as sa
import cx_Oracle as oracle

import sqlparse as spa

from .config import config


class Database:
    """Represent database with application schema."""

    ckwargs = {'literal_binds': True}

    def __init__(self):
        if self.configured:
            self.setup()
            self.load()
            self.formatter = self.Formatter(self)

    class Tables:
        """Represents database tables."""

        def __init__(self, database):
            self.database = database
            self.config = self.load('rapo_config')
            self.config_bak = self.load('rapo_config_bak')
            self.log = self.load('rapo_log')
            self.scheduler = self.load('rapo_scheduler')
            self.web_api = self.load('rapo_web_api')
            self.checkpoint = self.initiate('rapo_checkpoint')

        def load(self, name):
            """Load one of database table by name.

            Returns
            -------
            table : sqlalchemy.Table
                Database table instance.
            """
            meta = sa.MetaData()
            engine = self.database.engine
            table = sa.Table(name, meta, autoload=True, autoload_with=engine)
            return table

        def initiate(self, name):
            """Initiate one of databse table by name."""
            if not self.check(name):
                structure_name = name.replace('rapo_', '')
                structure = self.database.structure.get(structure_name)
                structure.create(self.database.engine)
            return self.load(name)

        def check(self, name):
            """Check if specified table exists by name."""
            if self.database.engine.has_table(name):
                return True
            return False

    class Structure:
        """Represents database tables structure."""

        def __init__(self, database):
            self.database = database
            self.metadata = sa.MetaData()

        def get(self, name):
            """Get database table structure by its name."""
            return getattr(self, name)

        @property
        def checkpoint(self):
            return sa.Table(
                'rapo_checkpoint', self.metadata,
                sa.Column('control_id', sa.Integer, nullable=False),
                sa.Column('process_id', sa.Integer, nullable=False),
                sa.Column('added', sa.Date, nullable=False),
                sa.UniqueConstraint('control_id', name='rapo_checkpoint_uq')
            )

    class Formatter:
        """Represents database query formatter."""

        def __init__(self, database):
            self.database = database

        def __call__(self, statement):
            """Format given SQL statement to match the common style."""
            string = self._compile(statement)
            result = self._parse(string)
            return result

        def document(self, *statements):
            """Format given SQL statements for documents or logging."""
            results = []
            for statement in statements:
                text = self._prepare(statement)
                result = self._parse(text)
                results.append(result)
            body = '\n'.join(results)
            result = f'{str():->40}\n{body}\n{str():->40}'
            return result

        def _prepare(self, statement):
            string = self._compile(statement)
            result = self._separate(string)
            return result

        def _parse(self, statement):
            result = spa.format(statement, keyword_case='upper',
                                identifier_case='lower',
                                reindent_aligned=True)
            return result

        def _compile(self, statement):
            if isinstance(statement, str):
                return statement
            else:
                engine = self.database.engine
                ckwargs = self.database.ckwargs
                statement = statement.compile(bind=engine,
                                              compile_kwargs=ckwargs)
                result = statement.string
                return result

        def _separate(self, statement):
            if statement.rstrip()[-1] != ';':
                statement = f'{statement.rstrip()};'
            return statement

    def setup(self):
        """Setup database engine based on configuration."""
        parameters = config['DATABASE']
        vendor_name = parameters.get_deprecated('vendor', 'vendor_name')
        driver_name = parameters.get('driver_name')
        host = parameters.get('host')
        port = parameters.get('port')
        path = parameters.get('path')
        sid = parameters.get('sid')
        service_name = parameters.get_deprecated('service', 'service_name')
        username = parameters.get_deprecated('user', 'username')
        password = parameters.get('password')
        client_path = parameters.get('client_path')
        max_identifier_length = parameters.get('max_identifier_length', 128)
        max_overflow = parameters.get('max_overflow', 10)
        pool_pre_ping = parameters.get('pool_pre_ping', True)
        pool_size = parameters.get('pool_size', 5)
        pool_recycle = parameters.get('pool_recycle', -1)
        pool_timeout = parameters.get('pool_timeout', 30)
        if vendor_name == 'sqlite' and path:
            if sys.platform.startswith('win'):
                url = f'{vendor_name}:///{path}'
            else:
                url = f'{vendor_name}:////{path}'
            settings = {}
        elif vendor_name == 'oracle':
            credentials = f'{username}:{password}'
            address = f'{host}:{port}'
            identifier = sid if sid else f'?service_name={service_name}'
            if driver_name:
                url = f'{vendor_name}+{driver_name}://{credentials}@{address}'
            else:
                url = f'{vendor_name}://{credentials}@{address}'
            url += f'/{identifier}'
            if client_path:
                oracle.init_oracle_client(lib_dir=client_path)
            settings = dict(max_identifier_length=max_identifier_length,
                            max_overflow=max_overflow,
                            pool_pre_ping=pool_pre_ping,
                            pool_size=pool_size,
                            pool_recycle=pool_recycle,
                            pool_timeout=pool_timeout)
        self.engine = sa.create_engine(url, **settings)

    def load(self):
        """Read database objects into memory."""
        self.structure = self.Structure(self)
        self.tables = self.Tables(self)

    def connect(self):
        """Get database connection.

        Returns
        -------
        connection : sqlalchemy.Connection
            Database connection instance.
        """
        connection = self.engine.connect()
        return connection

    def execute(self, statement, auto_commit=True, return_connection=False,
                as_records=False, as_table=False, as_one=False, as_dict=False,
                as_scalar=False, as_generator=False, output=None, tag=None):
        """Execute given SQL statement.

        Returns
        -------
        result : sqlalchemy.engine.CursorResult
            Execution result object.
        """
        try:
            if output:
                document = self.formatter.document(statement)
                tag = f'{tag} ' if tag else ''
                message = f'{tag}Running query:\n{document}'
                output(message)
            connection = self.connect()
            transaction = connection.begin()
            result = connection.execute(statement)
            if as_records:
                result = [record for record in result]
            elif as_table:
                result = [dict(record) for record in result]
            elif as_one or as_dict:
                result = result.first()
                if result and as_dict:
                    result = dict(result)
            elif as_scalar:
                result = result.scalar()
            if as_generator and (as_records or as_table):
                result = map(lambda record: record, result)
        except Exception as error:
            try:
                if auto_commit:
                    transaction.rollback()
                connection.close()
            finally:
                raise error
        else:
            if return_connection:
                return result, connection, transaction
            try:
                if auto_commit:
                    transaction.commit()
                connection.close()
            finally:
                return result

    def execute_many(self, *statements, result_queue=None, **kwargs):
        """Execute given SQL statements.

        Returns
        -------
        result_list : list of sqlalchemy.engine.CursorResult
            Execution result objects in order of initial statements.
        """
        result_list = []
        for statement in statements:
            result = self.execute(statement, **kwargs)
            result_list.append(result)
        if result_queue:
            result_queue.put(result_list)
        return result_list

    def parallelize(self, *statement_groups, **kwargs):
        """Execute given SQL statement groups in parallel threads.

        Returns
        -------
        result_groups : list of sqlalchemy.engine.CursorResult
            Execution result objects in structure of initial statement groups.
        """
        current_thread = th.current_thread()
        thread_list = []
        result_queue = qe.Queue()
        for statement_group in statement_groups:
            action_name = statement_group['name']
            statement_list = statement_group['statements']
            thread_name = f'{current_thread.name}({action_name})'
            thread = th.Thread(target=self.execute_many,
                               name=thread_name,
                               args=statement_list,
                               kwargs={'result_queue': result_queue,
                                       **kwargs})
            thread.start()
            thread_list.append(thread)
        for thread in thread_list:
            thread.join()
        # result_groups = list(result_queue)
        return result_queue

    def table(self, name):
        """Get database table.

        Returns
        -------
        table : sqlalchemy.Table
            Database table instance.
        """
        meta = sa.MetaData()
        table = sa.Table(name, meta, autoload=True, autoload_with=self.engine)
        return table

    def exists(self, table_name):
        """Check if the specified table exists by name."""
        return self.tables.check(table_name)

    def drop(self, table_name):
        """Delete database table by name.

        Parameters
        ----------
        table_name : str
            Name of the database table to be dropped.
        """
        if self.is_table(table_name):
            if self.is_materialized_view(table_name):
                query = f'drop materialized view {table_name}'
            else:
                query = f'drop table {table_name}'
            self.execute(query)

    def purge(self, table_name):
        """Delete database table by name permanently.

        Parameters
        ----------
        table_name : str
            Name of the database table to be dropped.
        """
        if self.is_table(table_name):
            if self.is_materialized_view(table_name):
                query = f'drop materialized view {table_name}'
            else:
                query = f'drop table {table_name} purge'
            self.execute(query)

    def truncate(self, table_name):
        """Clean database table by name.

        Parameters
        ----------
        table_name : str
            Name of the database table to be cleaned.
        """
        query = f'truncate table {table_name}'
        self.execute(query)

    def get_table_type(self, table_name):
        """Get the table type by name.

        Parameters
        ----------
        table_name : str
            Name of the database table to be checked.
        """
        table_name = table_name.upper()
        query = ('select object_type from user_objects '
                 f'where object_name = \'{table_name}\' '
                 'and object_type in (\'TABLE\', \'VIEW\')')
        table_type = self.execute(query, as_scalar=True)
        return table_type

    def get_view_type(self, view_name):
        """Get the view type by name.

        Parameters
        ----------
        table_name : str
            Name of the database table to be checked.
        """
        view_name = view_name.upper()
        query = ('select object_type from user_objects '
                 f'where object_name = \'{view_name}\' '
                 'and object_type in (\'VIEW\', \'MATERIALIZED VIEW\')')
        view_type = self.execute(query, as_scalar=True)
        return view_type

    def is_table(self, table):
        """Check if the given object is a table.

        Parameters
        ----------
        table : str or Table
            Object to be checked.
        """
        table_name = table if isinstance(table, str) else table.name
        table_type = self.get_table_type(table_name)
        return True if table_type == 'TABLE' else False

    def is_view(self, table):
        """Check if the given object is a view.

        Parameters
        ----------
        table : str or Table
            Object to be checked.
        """
        table_name = table if isinstance(table, str) else table.name
        table_type = self.get_table_type(table_name)
        return True if table_type == 'VIEW' else False

    def is_materialized_view(self, view):
        """Check if the given object is a materialized view.

        Parameters
        ----------
        table : str or Table
            Object to be checked.
        """
        view_name = view if isinstance(view, str) else view.name
        view_type = self.get_view_type(view_name)
        return True if view_type == 'MATERIALIZED VIEW' else False

    def get_column_type(self, table_name, column_name):
        table_name = table_name.upper()
        column_name = column_name.upper()
        query = ('select data_type from user_tab_columns '
                 f'where table_name = \'{table_name}\' '
                 f'and column_name = \'{column_name}\'')
        column_type = self.execute(query, as_scalar=True)
        return column_type

    def is_date(self, table, column):
        """Check if the given column is a DATE."""
        table_name = table if isinstance(table, str) else table.name
        column_name = column if isinstance(column, str) else column.name
        column_type = self.get_column_type(table_name, column_name)
        return True if column_type == 'DATE' else False

    def is_timestamp(self, table, column):
        """Check if the given column is a TIMESTAMP."""
        table_name = table if isinstance(table, str) else table.name
        column_name = column if isinstance(column, str) else column.name
        column_type = self.get_column_type(table_name, column_name)
        return True if column_type.startswith('TIMESTAMP') else False

    def get_rowid(self, field_name):
        """Get rowid column named by alias."""
        return sa.literal_column('rowid').label(field_name)

    def format(self, statement):
        """Format given SQL statement through the formatter.

        Parameters
        ----------
        statement : str
            Initial SQL statement.

        Returns
        -------
        statement : str
            Formatted SQL statement.
        """
        return self.formatter(statement)

    def compile(self, obj):
        """Compile given SQL object into a string using the engine.

        Parameters
        ----------
        obj : sqlalchemy statement
            Initial SQL object.

        Returns
        -------
        statement : str
            Compiled SQL statement.
        """
        return obj.compile(bind=self.engine, compile_kwargs=self.ckwargs)

    def normalize(self, columns, date_fields=[]):
        """Normalize list of columns."""
        normalized_columns = []
        for column in columns:
            if date_fields and column.name in date_fields:
                if self.is_timestamp(column.table, column):
                    column = sa.cast(column, sa.DATE).label(column.name)
            normalized_columns.append(column)
        return normalized_columns

    def cleanup(self):
        """Clean up stale or completed processes from all tables.

        This function removes records from the checkpoint table based on the
        following rules:

        1. Any checkpoint entry with corresponding process in logs has a status
           indicating it is finished (e.g., D - Done, C - Canceled, X -
           Deleted).

        2. Any checkpoint entry with date older than the corresponding start
           date in either the scheduler or api tables, indicating the process
           has likely been restarted or superseded.

        This cleanup ensures that the checkpoint table only contains active or
        pending processes that are still valid and helps avoid race conditions
        or stale locks.
        """
        checkpoint = self.tables.checkpoint
        logs = self.tables.log
        scheduler = self.tables.scheduler
        api = self.tables.web_api

        finished_status = ['D', 'E', 'C', 'X']
        finished_processes = (
            sa.select(checkpoint.c.control_id, checkpoint.c.process_id)
              .select_from(
                  sa.join(
                      checkpoint, logs,
                      checkpoint.c.process_id == logs.c.process_id
                  )
              ).where(logs.c.status.in_(finished_status))
        )
        finished_checkpoints = sa.exists(
            finished_processes.where(
                sa.and_(
                    checkpoint.c.control_id == finished_processes.c.control_id,
                    checkpoint.c.process_id == finished_processes.c.process_id
                )
            )
        )
        scheduler_reboot = sa.exists(
            sa.select(scheduler.c.id, scheduler.c.start_date).where(
                checkpoint.c.added < scheduler.c.start_date
            )
        )
        api_reboot = sa.exists(
            sa.select(api.c.id, api.c.start_date).where(
                checkpoint.c.added < api.c.start_date
            )
        )
        delete = sa.delete(checkpoint).where(
            sa.or_(
                finished_checkpoints,
                scheduler_reboot,
                api_reboot
            )
        )
        self.execute(delete)

    @property
    def configured(self):
        """Test database configuration."""
        if config and config.check('DATABASE'):
            parameters = config['DATABASE']
            vendor_name = parameters.get('vendor_name',
                                         parameters.get('vendor'))
            if vendor_name:
                host = parameters.get('host')
                port = parameters.get('port')
                path = parameters.get('path')
                if vendor_name == 'sqlite':
                    if path and os.path.exists(path):
                        return True
                    return False
                elif vendor_name == 'oracle':
                    if host and port:
                        return True
                    return False
        return False


db = Database()
