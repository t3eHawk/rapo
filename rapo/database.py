"""Contains database instance with application schema."""

import sys

import sqlalchemy as sa
import sqlparse as spe

from .config import config


class Database():
    """Represent database with application schema."""

    ckwargs = {'literal_binds': True}

    def __init__(self):
        vendor = config['DATABASE']['vendor']
        host = config['DATABASE']['host']
        port = config['DATABASE']['port']
        path = config['DATABASE'].get('path')
        sid = config['DATABASE'].get('sid')
        service = config['DATABASE'].get('service')
        user = config['DATABASE']['user']
        password = config['DATABASE']['password']
        if vendor == 'sqlite' and path is not None:
            if sys.platform.startswith('win'):
                url = f'{vendor}:///{path}'
            else:
                url = f'{vendor}:////{path}'
        else:
            url = f'{vendor}://{user}:{password}@{host}:{port}'
            if sid is not None:
                url += f'/{sid}'
            elif service is not None:
                url += f'/?service_name={service}'
        settings = dict(pool_pre_ping=True, max_identifier_length=128)
        self.engine = sa.create_engine(url, **settings)
        self.tables = self.Tables(self)
        self.formatter = self.Formatter(self)
        pass

    class Tables():
        """Represents database tables."""

        def __init__(self, database):
            self.database = database
            self.config = self.load('rapo_config')
            self.log = self.load('rapo_log')
            self.scheduler = self.load('rapo_scheduler')
            self.web_api = self.load('rapo_web_api')
            pass

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

        pass

    class Formatter():
        """Represents database query formatter."""

        def __init__(self, database):
            self.database = database
            pass

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
            pass

        def _separate(self, query):
            if query.rstrip()[-1] != ';':
                query = f'{query.rstrip()};'
            return query

        pass

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

    pass


db = Database()
