import sqlalchemy as sql

from .config import config
from .logger import logger


class Database():
    ckwargs = {'literal_binds': True}

    def __init__(self):
        try:
            logger.debug('Getting configuration...')
            vendor = config['DATABASE']['vendor']
            host = config['DATABASE']['host']
            port = config['DATABASE']['port']
            sid = config['DATABASE']['sid']
            user = config['DATABASE']['user']
            password = config['DATABASE']['password']
        except:
            logger.error()
        else:
            logger.debug('Configuration successfully retrieved')
        try:
            logger.debug('Creating database engine...')
            string = f'{vendor}://{user}:{password}@{host}:{port}/{sid}'
            self.engine = sql.create_engine(string)
        except:
            logger.error()
        else:
            logger.debug('Database engine created')
        pass

    def connect(self):
        try:
            logger.debug('Connecting to database...')
            connection = self.engine.connect()
        except:
            logger.critical()
        else:
            logger.debug('Successfully connected to database')
            return connection

    def table(self, name):
        conn = self.engine.connect()
        meta = sql.MetaData()
        return sql.Table(name, meta, autoload=True, autoload_with=db.engine)

db = Database()
