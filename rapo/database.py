"""Contains database instance with application schema."""

import sys

import sqlalchemy as sa
import cx_Oracle as oracle

import sqlparse as spe

from .config import config


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

        def __call__(self, *queries):
            """Format received raw SQL query into final human-read form."""
            results = []
            for query in queries:
                query = self._prepare(query)
                result = self._parse(query)
                results.append(result)
            strings = '\n'.join(results)
            string = f'{str():->40}\n{strings}\n{str():->40}'
            return string

        def _prepare(self, query):
            query = self._to_string(query)
            query = self._separate(query)
            return query

        def _parse(self, query):
            result = spe.format(query, keyword_case='upper',
                                identifier_case='lower',
                                reindent_aligned=True)
            return result

        def _to_string(self, query):
            if isinstance(query, str):
                return query
            else:
                engine = self.database.engine
                ckwargs = self.database.ckwargs
                query = query.compile(bind=engine, compile_kwargs=ckwargs)
                string = query.string
                return string

        def _separate(self, query):
            if query.rstrip()[-1] != ';':
                query = f'{query.rstrip()};'
            return query

    def setup(self):
        """Setup database engine based on configuration."""
        used_config = config['DATABASE']
        try:
            vendor_name = used_config['vendor_name']
        except KeyError as error:
            if used_config.get('vendor'):
                vendor_name = used_config['vendor']
                print('Please use parameter vendor_name instead of vendor, ',
                      'which is depreciated and will be removed soon.')
            else:
                raise error
        driver_name = used_config.get('driver_name')
        host = used_config.get('host')
        port = used_config.get('port')
        path = used_config.get('path')
        sid = used_config.get('sid')
        service_name = used_config.get('service')
        service_name = used_config.get('service_name')
        try:
            username = used_config.get('username')
        except KeyError as error:
            if used_config.get('user'):
                username = used_config.get('user')
                print('Please use parameter username instead of user, ',
                      'which is depreciated and will be removed soon.')
            else:
                raise error
        password = used_config.get('password')
        client_path = used_config.get('client_path')
        max_identifier_length = used_config.get('max_identifier_length', 128)
        max_overflow = used_config.get('max_overflow', 10)
        pool_pre_ping = used_config.get('pool_pre_ping', True)
        pool_size = used_config.get('pool_size', 5)
        pool_recycle = used_config.get('pool_recycle', -1)
        pool_timeout = used_config.get('pool_timeout', 30)
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


db = Database()
