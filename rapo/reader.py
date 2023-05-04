"""Contains application data reader."""

import sqlalchemy as sa

from .database import db


class Reader():
    """Represents application data reader.

    Reader used to extract aplication data from database to user.
    """

    def read_scheduler_record(self):
        """Get scheduler record from DB table.

        Returns
        -------
        record : dict
            Ordinary dictionary with web API information from DB table.
        """
        conn = db.connect()
        table = db.tables.scheduler
        select = table.select()
        result = conn.execute(select).first()
        record = dict(result)
        return record

    def read_web_api_record(self):
        """Get web API record from DB table.

        Returns
        -------
        record : dict
            Ordinary dictionary with web API information from DB table.
        """
        conn = db.connect()
        table = db.tables.web_api
        select = table.select()
        result = conn.execute(select).first()
        record = dict(result)
        return record

    def read_control_name(self, process_id):
        """Get control name using passed process_id.

        Parameters
        ----------
        process_id : int
            Unique process ID used to load record from DB log.

        Returns
        -------
        control_name : str
            Name of the defined control.
        """
        conn = db.connect()
        log = db.tables.log
        config = db.tables.config
        join = log.join(config, log.c.control_id == config.c.control_id)
        select = (sa.select([config.c.control_name]).select_from(join)
                    .where(log.c.process_id == process_id))
        result = conn.execute(select).first()
        control_name = result.control_name
        return control_name

    def read_control_result(self, process_id):
        """Get control result from DB log table.

        Parameters
        ----------
        process_id : int
            Unique process ID used to load record from DB log.

        Returns
        -------
        record : dict
            Ordinary dictionary with control result.
        """
        if process_id:
            conn = db.connect()
            table = db.tables.log
            select = table.select().where(table.c.process_id == process_id)
            result = conn.execute(select).first()
            if result:
                record = dict(result)
                return record
            else:
                message = f'no process with ID {process_id} found'
                raise ValueError(message)
        else:
            return None

    def read_control_config(self, control_name):
        """Get dictionary with control configuration from DB.

        Parameters
        ----------
        control_name : str
            Unique control name used to load record from DB configuration.

        Returns
        -------
        record : dict
            Ordinary dictionary with control configuration.
        """
        conn = db.connect()
        table = db.tables.config
        select = table.select().where(table.c.control_name == control_name)
        result = conn.execute(select).first()
        if result:
            record = dict(result)
            return record
        else:
            message = f'no control with name {control_name} found'
            raise ValueError(message)

    def read_running_controls(self):
        """Get list of running controls."""
        conn = db.connect()
        table = db.tables.log
        select = table.select().where(table.c.status == 'P')
        result = conn.execute(select)
        rows = [dict(row) for row in result]
        return rows


reader = Reader()
