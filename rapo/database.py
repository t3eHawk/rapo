"""Contains database instance with application schema."""

import sys

import sqlalchemy as sa
import cx_Oracle as oracle

import sqlparse as spa

from .config import config
from .config import get_deprecated


class Database():
    """Represent database with application schema."""

    ckwargs = {'literal_binds': True}

    def __init__(self):
        self.setup()
        self.load()
        self.formatter = self.Formatter(self)

    class Tables():
        """Represents database tables."""

        def __init__(self, database):
            self.database = database
            self.config = self.load('rapo_config')
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
        vendor_name = get_deprecated(parameters, 'vendor', 'vendor_name')
        driver_name = parameters.get('driver_name')
        host = parameters.get('host')
        port = parameters.get('port')
        path = parameters.get('path')
        sid = parameters.get('sid')
        service_name = get_deprecated(parameters, 'service', 'service_name',
                                      optional=True)
        username = get_deprecated(parameters, 'user', 'username')
        password = parameters.get('password')
        client_path = parameters.get('client_path')
        max_identifier_length = parameters.getint('max_identifier_length', 128)
        max_overflow = parameters.getint('max_overflow', 10)
        pool_pre_ping = parameters.getboolean('pool_pre_ping', True)
        pool_size = parameters.getint('pool_size', 5)
        pool_recycle = parameters.getint('pool_recycle', -1)
        pool_timeout = parameters.getint('pool_timeout', 30)
        if vendor_name == 'sqlite' and path:
            if sys.platform.startswith('win'):
                url = f'{vendor_name}:///{path}'
            else:
                url = f'{vendor_name}:////{path}'
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

    def execute(self, statement):
        """Execute given SQL statement.

        Returns
        -------
        result : sqlalchemy.engine.CursorResult
            Execution result object.
        """
        try:
            conn = self.connect()
            result = conn.execute(statement)
        except Exception as error:
            conn.close()
            raise error
        else:
            conn.close()
            return result

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
        """Drop database table by name.

        Parameters
        ----------
        table_name : str
            Name of the database table to be dropped.
        """
        query = f'drop table {table_name}'
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


db = Database()
