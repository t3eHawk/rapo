"""Database.

Contains database instance used for connections.
"""

import sqlalchemy as sql

from .config import config


class Database():
    """Class represent database."""

    ckwargs = {'literal_binds': True}

    def __init__(self):
        if config.has_section('DATABASE') is True:
            vendor = config['DATABASE']['vendor']
            host = config['DATABASE']['host']
            port = config['DATABASE']['port']
            sid = config['DATABASE']['sid']
            user = config['DATABASE']['user']
            password = config['DATABASE']['password']
            string = f'{vendor}://{user}:{password}@{host}:{port}/{sid}'
            self.engine = sql.create_engine(string)
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
