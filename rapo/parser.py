"""Contains application data parser."""

import datetime as dt
import json
import sqlalchemy as sql

from .database import db
from .logger import logger
from .utils import utils


class Parser():
    """Represents application parser.

    Parser used to transform input data from RAPO_CONFIG to output
    appropriate for application.
    """

    def __init__(self, bind):
        self.__bind = bind
        pass

    @property
    def control(self):
        """Get binded control instance."""
        return self.__bind

    @property
    def c(self):
        """Get binded control instance. Same as control property."""
        return self.__bind

    def parse_log(self):
        """Get RAPO_LOG table.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting RAPO_LOG table.
        """
        logger.debug(f'{self.c} Getting RAPO_LOG...')
        tablename = 'rapo_log'
        table = db.table(tablename)
        logger.debug(f'{self.c} RAPO_LOG loaded')
        return table

    def parse_config(self):
        """Get RAPO_CONFIG table.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting RAPO_CONFIG table.
        """
        control_name = self.control.name
        logger.debug(f'{self.c} Getting RAPO_CONFIG...')
        conn = db.connect()
        tablename = 'rapo_config'
        table = db.table(tablename)
        select = table.select().where(table.c.control_name == control_name)
        record = conn.execute(select).first()
        if record is None:
            raise ValueError(f'no control with a name {control_name} found')
        else:
            logger.debug(f'{self.c} RAPO_CONFIG loaded')
            return record

    def parse_table_source(self):
        """Get data source table.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting data source table.
        """
        name = self.control.source_name
        table = self._parse_table_source(name)
        return table

    def parse_table_source_a(self):
        """Get data source A table.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting data source A table.
        """
        name = self.control.source_name_a
        table = self._parse_table_source(name)
        return table

    def parse_table_source_b(self):
        """Get data source B table.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting data source B table.
        """
        name = self.control.source_name_b
        table = self._parse_table_source(name)
        return table

    def parse_date_from(self, date_from):
        """Get data source date lower bound.

        Returns
        -------
        date_from : datetime or None
            Fetched records from data source should begin from this date.
        """
        date_from = self._parse_date(date_from, hour=0, minute=0, second=0)
        return date_from

    def parse_date_to(self, date_to):
        """Get data source date upper bound.

        date_to : datetime or None
            Fetched records from data source should end with this date.
        """
        date_to = self._parse_date(date_to, hour=23, minute=59, second=59)
        return date_to

    def parse_select(self):
        """Get SQL statement to fetch data from data source.

        Returns
        -------
        select : sqlalchemy.Select
        """
        table = self.control.table_source
        date_field = self.control.source_date_field
        select = self._parse_select(table, date_field=date_field)
        return select

    def parse_select_a(self):
        """Get SQL statement to fetch data from data source A.

        Returns
        -------
        select : sqlalchemy.Select
        """
        table = self.control.table_source_a
        date_field = self.control.source_date_field_a
        select = self._parse_select(table, date_field=date_field)
        return select

    def parse_select_b(self):
        """Get SQL statement to fetch data from data source B.

        Returns
        -------
        select : sqlalchemy.Select
        """
        table = self.control.table_source_b
        date_field = self.control.source_date_field_b
        select = self._parse_select(table, date_field=date_field)
        return select

    def parse_match_config(self):
        """Get control match configuration.

        Returns
        -------
        config : list
            List with dictionaries where presented all keys for matching.
        """
        logger.debug(f'{self.c} Parsing match configuration...')
        raw = self.control.config.match_config
        config = []
        for item in json.loads(raw or '[]'):
            column_a = item['column_a'].lower()
            column_b = item['column_b'].lower()

            new = {'column_a': column_a, 'column_b': column_b}
            config.append(new)
        logger.debug(f'{self.c} Match configuration parsed')
        return config

    def parse_mismatch_config(self):
        """Get control mismatch configuration.

        Returns
        -------
        config : list
            List with dictionaries where presented all keys for mismatching.
        """
        logger.debug(f'{self.c} Parsing mismatch configuration...')
        raw = self.control.config.mismatch_config
        config = []
        for item in json.loads(raw or '[]'):
            column_a = item['column_a'].lower()
            column_b = item['column_b'].lower()

            new = {'column_a': column_a, 'column_b': column_b}
            config.append(new)
        logger.debug(f'{self.c} Mismatch configuration parsed')
        return config

    def parse_error_config(self):
        """Get control error configuration.

        Returns
        -------
        config : list
            List with dictionaries where presented all keys for discrapancies.
        """
        logger.debug(f'{self.c} Parsing error configuration...')
        raw = self.control.config.error_config
        config = []
        for item in json.loads(raw or '[]'):
            connexion = item.get('connexion', 'and').upper()
            column = item.get('column', '').lower()
            column_a = item.get('column_a', '').lower()
            column_b = item.get('column_b', '').lower()
            relation = item.get('relation', '<>').upper()
            value = item.get('value')
            is_column = item.get('is_column', False)

            config.append(
                {'connexion': connexion,
                 'column': column or None,
                 'column_a': column_a or None,
                 'column_b': column_b or None,
                 'relation': relation,
                 'value': value.lower() if is_column is True else value,
                 'is_column': is_column})
        logger.debug(f'{self.c} Error configuration parsed')
        return config

    def parse_output_columns(self):
        """Get control output columns.

        Returns
        -------
        columns : list or None
            List with dictionaries where output columns configuration is
            presented.
            Will be None if configuration is not filled in RAPO_CONFIG.
        """
        config = self.control.config.output_table
        columns = self._parse_output_columns(config)
        return columns

    def parse_output_columns_a(self):
        """Get control output columns A.

        Returns
        -------
        columns : list or None
            List with dictionaries where output A columns configuration is
            presented.
            Will be None if configuration is not filled in RAPO_CONFIG.
        """
        config = self.control.config.output_table_a
        columns = self._parse_output_columns(config)
        return columns

    def parse_output_columns_b(self):
        """Get control output columns B.

        Returns
        -------
        columns : list or None
            List with dictionaries where output B columns configuration is
            presented.
            Will be None if configuration is not filled in RAPO_CONFIG.
        """
        config = self.control.config.output_table_b
        columns = self._parse_output_columns(config)
        return columns

    def _parse_table_source(self, name):
        logger.debug(f'{self.c} Parsing source table {name}...')
        if isinstance(name, str) is True or name is None:
            table = db.table(name) if isinstance(name, str) is True else None
            if table is None:
                raise AttributeError(f'Source table {name} is not defined')
            else:
                logger.debug(f'{self.c} Source table {name} parsed')
                return table
        else:
            raise TypeError('source name must be str or NoneType '
                            f'not {name.__class__.__name__}')

    def _parse_date(self, date, hour, minute, second):
        logger.debug(f'{self.c} Parsing date {date}...')
        trigger = self.control.trigger
        if isinstance(trigger, float) is True:
            logger.debug(f'{self.c} date trigger is used')
            days_back = self.control.config.days_back
            datetime = dt.datetime.fromtimestamp(trigger)
            datetime = datetime-dt.timedelta(days=days_back)
            logger.debug(f'{self.c} so {date} replaced with {datetime}')
            return datetime.replace(hour=hour, minute=minute, second=second)
        else:
            logger.debug(f'{self.c} date {date} parsed')
            return utils.to_date(date)

    def _parse_select(self, table, date_field=None):
        logger.debug(f'{self.c} Parsing {table} select...')
        select = table.select()
        if isinstance(date_field, str) is True:
            column = table.c[date_field]

            datefmt = '%Y-%m-%d %H:%M:%S'
            date_from = self.control.date_from.strftime(datefmt)
            date_to = self.control.date_to.strftime(datefmt)

            datefmt = 'YYYY-MM-DD HH24:MI:SS'
            date_from = sql.func.to_date(date_from, datefmt)
            date_to = sql.func.to_date(date_to, datefmt)

            select = select.where(column.between(date_from, date_to))
            logger.debug(f'{self.c} {table} select parsed')
        return select

    def _parse_output_columns(self, config):
        logger.debug(f'{self.c} Parsing output columns...')
        config = json.loads(config) if isinstance(config, str) else None
        if config is None:
            logger.debug(f'{self.c} No output was configured')
            return None
        else:
            column = dict.fromkeys(['column', 'column_a', 'column_b'])
            columns = []
            for value in config.get('columns', []):
                new = column.copy()
                if isinstance(value, str) is True:
                    new['column'] = value.lower()
                if isinstance(value, dict) is True:
                    for key in new.keys():
                        raw = value.get(key)
                        if isinstance(raw, str):
                            new[key] = raw.lower()
                columns.append(new)
            if columns:
                logger.debug(f'{self.c} Output columns parsed')
                return columns
            else:
                logger.debug(f'{self.c} No output columns was configured')
                return None
