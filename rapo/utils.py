"""Contains application utils."""

import datetime as dt
import json
import sqlalchemy as sa


class Utils():
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

utils = Utils()
