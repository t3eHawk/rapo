"""Contains database instance with application schema."""

import sys

import sqlalchemy as sql

from .config import config


class Database():
    """Represent database with application schema."""

    ckwargs = {'literal_binds': True}

    def __init__(self):
        if config.has_section('DATABASE') is True:
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
            self.engine = sql.create_engine(url)
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
        meta = sql.MetaData()
        return sql.Table(name, meta, autoload=True, autoload_with=db.engine)


db = Database()
