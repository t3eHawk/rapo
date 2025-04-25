"""Contains application utils."""

import os
import json
import datetime as dt
import calendar as cd
import sqlalchemy as sa


class Utils:
    """Represents application utils."""

    def to_date(self, input):
        """Normalize input value to datetime object.

        Parameters
        ----------
        input : str, datetime.datetime, None
            Initial value that should be normalized to datetime object.

        Returns
        -------
        output : datetime.datetime
            Nomalized datatime object.
        """
        if input is None:
            output = dt.datetime.now()
        elif isinstance(input, dt.datetime):
            output = input
        elif isinstance(input, str):
            output = dt.datetime.fromisoformat(input)
        elif isinstance(input, (int, float)):
            output = dt.datetime.fromtimestamp(input)
        return output

    def to_lower(self, value):
        """Lower string or return None.

        Parameters
        ----------
        value : str
            Initial value that must be modified.

        Returns
        -------
        value : str or None
            Modified value.
        """
        value = value.lower() if isinstance(value, str) is True else None
        return value

    def is_config(self, value):
        """Check if the given value is valid configuration object.

        Parameters
        ----------
        value : Any
            Some vale that must be checked.

        Returns
        -------
        result : bool
            Result of the check.
        """
        if isinstance(value, dict):
            return True
        else:
            return False

    def is_json(self, string):
        """Check if the given string is a valid JSON object.

        Parameters
        ----------
        string: str or Any
            Some string that must be checked.

        Returns
        -------
        result : bool
            Result of the check.
        """
        try:
            json.loads(string)
        except (TypeError, ValueError):
            return False
        else:
            return True

    def is_sql(self, string):
        """Check if the given string is a valid SQL object.

        Parameters
        ----------
        string : str or Any

        Returns
        -------
        result : bool
            Result of the check.
        """
        if isinstance(string, str):
            return True
        else:
            return False

    def is_sqlalchemy(self, value):
        """Check if the given value is a valid SQLAlchemy object.

        Parameters
        ----------
        value : str or Any

        Returns
        -------
        result : bool
            Result of the check.
        """
        if isinstance(value, sa.sql.ClauseElement):
            return True
        else:
            return False

    def get_config(self, config_id, config_list):
        """Get chosen iteration configuration by its number."""
        for item_config in config_list:
            key_name = list(item_config)[0]
            if key_name.endswith('id'):
                if item_config[key_name] == config_id:
                    return item_config

    def get_month_date_from(self, initial_date):
        """Get first month date from the given initial date."""
        calculated_date = initial_date.replace(day=1, hour=0,
                                               minute=0, second=0)
        return calculated_date

    def get_month_date_to(self, initial_date):
        """Get last month date from the given initial date."""
        month_range = cd.monthrange(initial_date.year, initial_date.month)
        last_day = month_range[1]
        calculated_date = initial_date.replace(day=last_day, hour=23,
                                               minute=59, second=59)
        return calculated_date

    def read_sql(self, module_relative_path):
        """Read the specified SQL file by its path.

        Parameters
        ----------
        module_relative_path : str
        Relative path to the SQL file from the directory,

        Returns
        -------
        text : bool
            Result of the check.
        """
        module_directory = os.path.dirname(__file__)
        file_path = f'{module_directory}/{module_relative_path}.sql'
        text = open(file_path, 'r', encoding='utf-8').read()
        return text


utils = Utils()
