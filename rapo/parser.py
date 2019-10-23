import datetime as dt
import json
import sqlalchemy as sql

from .database import db
from .logger import logger


class Parser():
    def __init__(self, bind):
        self.__bind = bind
        pass

    @property
    def control(self):
        return self.__bind

    def parse_log(self):
        logger.debug(f'{self.control} Getting RAPO_LOG...')
        tablename = 'rapo_log'
        table = db.table(tablename)
        logger.debug(f'{self.control} RAPO_LOG loaded')
        return table

    def parse_config(self):
        control_name = self.control.name
        logger.debug(f'{self.control} Getting RAPO_CONFIG...')
        conn = db.connect()
        tablename = 'rapo_config'
        table = db.table(tablename)
        select = table.select().where(table.c.control_name == control_name)
        record = conn.execute(select).first()
        if record is None:
            raise ValueError(f'no control with a name {control_name} found')
        else:
            logger.debug(f'{self.control} RAPO_CONFIG loaded')
            return record

    def parse_table_x(self):
        logger.debug(f'{self.control} Parsing source table X...')
        table_x = self._parse_table(self.control.source_x)
        if table_x is None:
            raise AttributeError('Source table X is not defined')
        else:
            logger.debug(f'{self.control} Source table X parsed')
            return table_x

    def parse_errors(self):
        logger.debug(f'{self.control} Parsing errors...')
        errors = []
        for item in json.loads(self.control.config.error):
            error = {}

            connexion = item.get('connexion', 'and').upper()
            column_x = item.get('column_x', '').lower()
            column_a = item.get('column_a', '').lower()
            column_b = item.get('column_b', '').lower()
            relation = item.get('relation', '<>').upper()
            value = item.get('value')
            is_column = item.get('is_column', False)

            error = {'connexion': connexion,
                     'column_x': column_x or None,
                     'column_a': column_a or None,
                     'column_b': column_b or None,
                     'relation': relation,
                     'value': value.lower() if is_column is True else value,
                     'is_column': is_column}
            errors.append(error)
        logger.debug(f'{self.control} errors parsed')
        return errors

    def parse_output_x(self):
        logger.debug(f'{self.control} Parsing output...')
        output_x = json.loads(self.control.config.output_x)
        if output_x is None:
            return None
        elif isinstance(output_x, dict) is True:
            columns = output_x.get('columns', [])
            columns = list(map(lambda e: e.lower(), columns))
            logger.debug(f'{self.control} Output parsed')
            return columns

    def to_lower(self, value):
        value = value.lower() if isinstance(value, str) is True else None
        return value

    def parse_date_from(self, date_from):
        trigger = self.control.trigger
        if isinstance(trigger, float) is True:
            days_back = self.control.config.days_back
            datetime = dt.datetime.fromtimestamp(trigger)
            datetime = datetime-dt.timedelta(days=days_back)
            return datetime.replace(hour=0, minute=0, second=0)
        else:
            return self.parse_date(date_from)

    def parse_date_to(self, date_to):
        trigger = self.control.trigger
        if isinstance(trigger, float) is True:
            days_back = self.control.config.days_back
            datetime = dt.datetime.fromtimestamp(trigger)
            datetime = datetime-dt.timedelta(days=days_back)
            return datetime.replace(hour=23, minute=59, second=59)
        else:
            return self.parse_date(date_to)

    def parse_date(self, input_):
        if input_ is None:
            output = dt.datetime.now()
        elif isinstance(input_, dt.datetime) is True:
            output = input_
        elif isinstance(input_, str) is True:
            output = dt.datetime.fromisoformat(input_)
        return output

    def _parse_table(self, name):
        if isinstance(name, str) is True:
            table = db.table(name)
            return table
        elif name is None:
            return None
        else:
            raise TypeError('source name must be str or NoneType '
                            f'not {name.__class__.__name__}')
