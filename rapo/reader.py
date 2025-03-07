"""Contains application data reader."""

import sqlalchemy as sa

from .database import db

from datetime import datetime


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
        table = db.tables.scheduler
        select = table.select()
        result = db.execute(select).first()
        record = dict(result)
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
        result = db.execute(select).first()
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
        log = db.tables.log
        config = db.tables.config
        join = log.join(config, log.c.control_id == config.c.control_id)
        select = (sa.select([config.c.control_name]).select_from(join)
                    .where(log.c.process_id == process_id))
        result = db.execute(select).first()
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
            result = db.execute(select).first()
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
        table = db.tables.config
        select = table.select().where(table.c.control_name == control_name)
        result = db.execute(select).first()
        if result:
            record = dict(result)
            return record
        else:
            message = f'no control with name {control_name} found'
            raise ValueError(message)

    def read_running_controls(self):
        """Get list of running controls."""
        table = db.tables.log
        select = table.select().where(table.c.status == 'P')
        result = db.execute(select)
        rows = [dict(row) for row in result]
        return rows

    def read_control_config_all(self):
        """Get list of all controls in the config table."""

        config = db.tables.config
        select = config.select().order_by(config.c.updated_date.desc())
        result = db.execute(select)
        rows = [dict(row) for row in result]
        return rows

    def read_control_config_versions(self, control_id):
        """Get an array  with all past control configurations from DB."""
        table = db.tables.config_bak
        select = table.select().where(table.c.control_id == control_id).order_by(table.c.audit_date.desc())
        result = db.execute(select)
        rows = [dict(row) for row in result]
        return rows

    def read_control_results_for_day(self):
        """Get list of all control runs for the passed for_day."""

        # log = db.tables.log
        # config = db.tables.config
        # join = log.join(config, log.c.control_id == config.c.control_id)
        # select = (join.select().where(sa.and_(log.c.start_date >= from_date, log.c.start_date <= to_date)))
        # select = sa.select([config.c.control_name, log]).select_from(join).where(sa.and_(log.c.start_date >= from_date, log.c.start_date <= to_date))

        select = """
                select
                    c.control_name,
                    c.control_id,
                    l.process_id,
                    nvl(l.start_date, l.added) start_date,
                    l.date_from,
                    l.date_to,
                    case
                        when l.status = 'I' then 'Initiated'
                        when l.status in ('S', 'P', 'F') then 'Running'
                        when l.status = 'D' then 'Success'
                        when l.status = 'E' then 'Error'
                        when l.status = 'X' then 'Revoked'
                        when nvl(l.status, 'C') = 'C' then 'Canceled'
                        else l.status
                    end status,
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
                fetch first 100 rows only
        """

        result = db.execute(select)
        rows = [dict(row) for row in result]
        return rows

    def read_datasources(self):
        """Get list of all datasources in the DB."""

        result = db.execute("select object_name from user_objects where object_type in ('VIEW', 'TABLE') order by 1")
        rows = [dict(row)['object_name'] for row in result]
        return rows

    def read_datasource_columns(self, datasource_name):
        """Get list of all column names of the passed datasource_name."""

        result = db.execute(f"select column_name from user_tab_cols where table_name = '{datasource_name}' order by column_id")
        rows = [dict(row)['column_name'] for row in result]
        return rows

    def read_datasource_date_columns(self, datasource_name):
        """Get list of all DATE type column names of the passed datasource_name."""

        result = db.execute(f"select column_name from user_tab_cols where table_name = '{datasource_name}' and (data_type = 'DATE' or data_type like 'TIMESTAMP%') order by column_id")
        rows = [dict(row)['column_name'] for row in result]
        return rows

    def save_control(self, data):
        """Create or update control object in the config table with passed control data."""
        config = db.tables.config

        # Remove control object columns that are not in DB table
        for k in [i for i in set(data.keys()).difference(config.columns.keys())]:
          del data[k]

        data['updated_date'] = datetime.now().date()

        if 'control_id' in data:
            data['created_date'] = datetime.strptime(data['created_date'], '%a, %d %b %Y %H:%M:%S %Z')
            update = config.update().where(config.c.control_id == data['control_id']).values(data)
            result = db.execute(update)
        else:
            data['created_date'] = datetime.now().date()
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
