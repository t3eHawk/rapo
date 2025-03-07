"""Contains database instance with application schema."""

import os
import sys
import threading as th
import queue as qe

import sqlalchemy as sa
import cx_Oracle as oracle

import sqlparse as spa

from .config import config


class Database():
    """Represent database with application schema."""

    ckwargs = {'literal_binds': True}

    def __init__(self):
        if self.configured:
            self.setup()
            self.load()
            self.formatter = self.Formatter(self)

    class Tables():
        """Represents database tables."""

        def __init__(self, database):
            self.database = database
            self.config = self.load('rapo_config')
            self.config_bak = self.load('rapo_config_bak')
            self.log = self.load('rapo_log')
            self.scheduler = self.load('rapo_scheduler')
            self.web_api = self.load('rapo_web_api')

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

    class Formatter():
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

    def execute(self, statement, output=None, tag=None):
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
            conn = self.connect()
            result = conn.execute(statement)
        except Exception as error:
            conn.close()
            raise error
        else:
            conn.close()
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

    def drop(self, table_name):
        """Delete database table by name.

        Parameters
        ----------
        table_name : str
            Name of the database table to be dropped.
        """
        query = f'drop table {table_name}'
        self.execute(query)

    def purge(self, table_name):
        """Delete database table by name permanently.

        Parameters
        ----------
        table_name : str
            Name of the database table to be dropped.
        """
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
