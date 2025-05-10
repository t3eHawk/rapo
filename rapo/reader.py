"""Contains application data reader."""

import datetime as dt
import sqlalchemy as sa

from .database import db


class Reader:
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
        table = db.tables.scheduler
        select = table.select()
        record = db.execute(select, as_dict=True)
        return record

    def read_web_api_record(self):
        """Get web API record from DB table.

        Returns
        -------
        record : dict
            Ordinary dictionary with web API information from DB table.
        """
        table = db.tables.web_api
        select = table.select()
        record = db.execute(select, as_dict=True)
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
        log = db.tables.log
        config = db.tables.config
        join = log.join(config, log.c.control_id == config.c.control_id)
        select = (sa.select([config.c.control_name]).select_from(join)
                    .where(log.c.process_id == process_id))
        result = db.execute(select, as_one=True)
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
            table = db.tables.log
            select = table.select().where(table.c.process_id == process_id)
            record = db.execute(select, as_dict=True)
            if record:
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
        table = db.tables.config
        select = table.select().where(table.c.control_name == control_name)
        record = db.execute(select, as_dict=True)
        if record:
            return record
        else:
            message = f'no control with name {control_name} found'
            raise ValueError(message)

    def read_control_logs(self, control_name, days=365, statuses=[],
                          order_by=True):
        """Retrieve control run logs with given parameters.

        Parameters
        ----------
        control_name : str
            Unique control name used to load logs.
        days : int
            Number of days for which logs are to be received.
        status : list
            List of statuses with which logs are to be retrieved.
        order_by : bool
            Whether to sort logs by process_id.

        Returns
        -------
        records : list
            Ordinary list with control logs.
        """
        log = db.tables.log
        config = db.tables.config
        join = log.join(config, log.c.control_id == config.c.control_id)
        select = sa.select(log.columns).select_from(join)\
                   .where(config.c.control_name == control_name)
        if days and isinstance(days, int):
            dateform = 'YYYY-MM-DD HH24:MI:SS'
            cut_off_date = dt.datetime.now()-dt.timedelta(days=days)
            cut_off_date = cut_off_date.strftime('%Y-%m-%d %H:%M:%S')
            cut_off_date = sa.func.to_date(cut_off_date, dateform)
            select = select.where(log.c.added > cut_off_date)
        if statuses:
            if all(isinstance(i, str) and len(i) == 1 for i in statuses):
                select = select.where(log.c.status.in_(statuses))
        if order_by:
            select = select.order_by(log.c.process_id.desc())
        answerset = db.execute(select, as_table=True)
        return answerset

    def read_control_recent_logs(self, control_name):
        """Retrieve logs of currently working control instances.

        Parameters
        ----------
        control_name : str
            Unique control name used to load logs.

        Returns
        -------
        records : list
            Ordinary list with currently running control run logs.
        """
        statuses = ['W', 'S', 'P', 'F']
        return self.read_control_logs(control_name, days=1, statuses=statuses)

    def read_running_controls(self):
        """Get list of running controls."""
        table = db.tables.log
        select = table.select().where(table.c.status == 'P')
        answerset = db.execute(select, as_table=True)
        return answerset

    def read_control_config_all(self):
        """Get list of all controls in the config table."""
        config = db.tables.config
        select = config.select().order_by(config.c.updated_date.desc())
        answerset = db.execute(select, as_table=True)
        return answerset

    def read_control_config_versions(self, control_id):
        """Get an array  with all past control configurations from DB."""
        table = db.tables.config_bak
        select = table.select().where(table.c.control_id == control_id).order_by(table.c.audit_date.desc())
        answerset = db.execute(select, as_table=True)
        return answerset

    def read_control_results_for_day(self):
        """Get list of all control runs for the passed for_day."""
        select = """
                select
                    c.control_name,
                    c.control_id,
                    c.control_type,
                    l.process_id,
                    nvl(l.start_date, l.added) start_date,
                    l.date_from,
                    l.date_to,
                    l.status,
                    nvl(coalesce(success_number_a, success_number), 0) as success_number_a,
                    nvl(coalesce(success_number_b, 0), 0) as success_number_b,
                    nvl(coalesce(fetched_number_a, fetched_number), 0) as fetched_number_a,
                    nvl(coalesce(fetched_number_b, 0), 0) as fetched_number_b,
                    nvl(coalesce(error_number_a, error_number), 0) as error_number_a,
                    nvl(coalesce(error_number_b, 0), 0) as error_number_b,
                    nvl(coalesce(error_level_a, error_level), 0) as error_level_a,
                    nvl(coalesce(error_level_b, 0), 0) as error_level_b,
                    text_log,
                    nvl(text_error, text_message) text_error,
                    prerequisite_value,
                    nvl(round((l.end_date - nvl(l.start_date, l.added)) * 1440, 2), 0) duration_minutes
                from rapo_log l
                left join rapo_config c on l.control_id = c.control_id
                where 1=1
                    and c.control_name is not null
                order by process_id desc
                fetch first 200 rows only
        """
        answerset = db.execute(select, as_table=True)
        return answerset

    def read_datasources(self):
        """Get list of all datasources in the DB."""
        answerset = db.execute("select object_name from user_objects where object_type in ('VIEW', 'TABLE') order by 1", as_table=True)
        object_names = [record['object_name'] for record in answerset]
        return object_names

    def read_datasource_columns(self, datasource_name):
        """Get list of all column names of the passed datasource_name."""

        answerset = db.execute(f"select column_name, data_type from user_tab_cols where table_name = '{datasource_name}' order by column_id", as_table=True)
        return answerset

    def save_control(self, data):
        """Create or update control object in the config table with passed control data."""
        config = db.tables.config

        # Remove control object columns that are not in DB table
        for k in [i for i in set(data.keys()).difference(config.columns.keys())]:
          del data[k]

        data['updated_date'] = dt.datetime.now()

        if 'control_id' in data:
            data['created_date'] = dt.datetime.strptime(data['created_date'], '%a, %d %b %Y %H:%M:%S %Z')
            update = config.update().where(config.c.control_id == data['control_id']).values(data)
            result = db.execute(update)
        else:
            data['created_date'] = dt.datetime.now()
            insert = config.insert().values(data)
            result = db.execute(insert)
        return result

    def delete_control(self, control_id):
        """Delete control from the config table of the passed control_id."""
        config = db.tables.config
        delete = config.delete().where(config.c.control_id == control_id)
        result = db.execute(delete)
        return result


reader = Reader()
