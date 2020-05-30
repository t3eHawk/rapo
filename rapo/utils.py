"""Contains application utils."""

import datetime as dt


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
        elif isinstance(input, dt.datetime) is True:
            output = input
        elif isinstance(input, str) is True:
            output = dt.datetime.fromisoformat(input)
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


utils = Utils()
