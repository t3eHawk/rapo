"""Contains database instance with application schema."""

import sys

import sqlalchemy as sa

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
        self.tables = self.Tables(database=self)
        pass

    class Tables():
        """Represents database tables."""

        def __init__(self, database=None):
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
