"""Contains executor responsible for control execution steps."""

import sqlalchemy as sql

from .database import db
from .logger import logger


class Executor():
    """Represents control executor."""

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

    def fetch_records(self):
        """Fetch data source.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with fetched data in case if DB is chosen
            engine.
        """
        select = self.control.select
        if self.control.engine == 'DB':
            tablename = f'rapo_temp_fd_{self.control.process_id}'
            table = self._fetch_records_to_table(select, tablename)
            return table

    def fetch_records_a(self):
        """Fetch data source A.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with fetched data in case if DB is chosen
            engine.
        """
        select = self.control.select_a
        if self.control.engine == 'DB':
            tablename = f'rapo_temp_fda_{self.control.process_id}'
            table = self._fetch_records_to_table(select, tablename)
            return table

    def fetch_records_b(self):
        """Fetch data source B.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with fetched data in case if DB is chosen
            engine.
        """
        select = self.control.select_b
        if self.control.engine == 'DB':
            tablename = f'rapo_temp_fdb_{self.control.process_id}'
            table = self._fetch_records_to_table(select, tablename)
            return table

    def analyze(self):
        """Run data analyze used for control with ANL type.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with found discrepancies in case if DB is
            chosen engine.
        """
        logger.debug(f'{self.c} Analyzing...')
        conn = db.connect()
        table_fetched = self.control.table_fetched
        output_columns = self.control.output_columns
        if output_columns is None or len(output_columns) == 0:
            select = table_fetched.select()
        else:
            columns = []
            for output_column in output_columns:
                name = output_column['column']
                column = table_fetched.c[name]
                columns.append(column)
            select = sql.select(columns)
        texts = []
        config = self.control.error_config
        for item in config:
            text = []
            connexion = item['connexion']
            if len(texts) > 0:
                text.append(connexion)
            column = str(table_fetched.c[item['column']])
            text.append(column)
            relation = item['relation']
            text.append(relation)
            value = item['value']
            is_column = item['is_column']
            value = str(table_fetched.c[value]) if is_column is True else value
            text.append(value)
            text = ' '.join(text)
            texts.append(text)
        select = select.where(sql.text('\n'.join(texts)))

        tablename = f'rapo_temp_err_{self.control.process_id}'
        select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
        ctas = sql.text(f'CREATE TABLE {tablename} AS \n{select}')
        logger.info(f'{self.c} Creating {tablename} with query \n{ctas}')
        conn.execute(ctas)
        logger.debug(f'{self.c} {tablename} created')

        table = db.table(tablename)
        logger.debug(f'{self.c} Analyzing done')
        return table

    def match(self):
        """Run data matching for control with REC type and MA method.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with matched data in case if DB is chosen
            engine.
        """
        logger.debug(f'{self.c} Matching...')

        conn = db.connect()
        table_a = self.control.table_fetched_a
        table_b = self.control.table_fetched_b

        columns = []
        output_columns = self.control.output_columns
        if output_columns is None or len(output_columns) == 0:
            columns.extend(table_a.columns)
            columns.extend(table_b.columns)
        else:
            for output_column in output_columns:
                name = output_column['column']

                column_a = output_column['column_a']
                column_a = table_a.c[column_a] if column_a else None

                column_b = output_column['column_b']
                column_b = table_b.c[column_b] if column_b else None

                if column_a is not None and column_b is not None:
                    column = sql.func.coalesce(column_a, column_b)
                elif column_a is not None:
                    column = column_a
                elif column_b is not None:
                    column = column_b

                column = column.label(name) if name else column
                columns.append(column)

        keys = []
        match_config = self.control.match_config
        for item in match_config:
            column_a = table_a.c[item['column_a']]
            column_b = table_b.c[item['column_b']]
            keys.append(column_a == column_b)
        join = table_a.join(table_b, *keys)
        select = sql.select(columns).select_from(join)

        keys = []
        mismatch_config = self.control.mismatch_config
        for item in mismatch_config:
            column_a = table_a.c[item['column_a']]
            column_b = table_b.c[item['column_b']]
            select = select.where(column_a == column_b)

        tablename = f'rapo_temp_ma_{self.control.process_id}'
        select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
        ctas = sql.text(f'CREATE TABLE {tablename} AS \n{select}')
        logger.info(f'{self.c} Creating {tablename} with query \n{ctas}')
        conn.execute(ctas)
        logger.debug(f'{self.c} {tablename} created')

        table = db.table(tablename)
        logger.debug(f'{self.c} Matching done')
        return table

    def mismatch(self):
        """Run data mismatching for control with REC type and MA method.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting table with mismatched data in case if DB is
            chosen engine.
        """
        logger.debug(f'{self.c} Mismatching...')

        conn = db.connect()
        table_a = self.control.table_fetched_a
        table_b = self.control.table_fetched_b

        columns = []
        output_columns = self.control.output_columns
        if output_columns is None or len(output_columns) == 0:
            columns.extend(table_a.columns)
            columns.extend(table_b.columns)
        else:
            for output_column in output_columns:
                name = output_column['column']

                column_a = output_column['column_a']
                column_a = table_a.c[column_a] if column_a else None

                column_b = output_column['column_b']
                column_b = table_b.c[column_b] if column_b else None

                if column_a is not None and column_b is not None:
                    column = sql.func.coalesce(column_a, column_b)
                elif column_a is not None:
                    column = column_a
                elif column_b is not None:
                    column = column_b

                column = column.label(name) if name else column
                columns.append(column)

        keys = []
        match_config = self.control.match_config
        for item in match_config:
            column_a = table_a.c[item['column_a']]
            column_b = table_b.c[item['column_b']]
            keys.append(column_a == column_b)
        join = table_a.join(table_b, *keys)
        select = sql.select(columns).select_from(join)

        keys = []
        mismatch_config = self.control.mismatch_config
        for item in mismatch_config:
            column_a = table_a.c[item['column_a']]
            column_b = table_b.c[item['column_b']]
            select = select.where(column_a != column_b)

        tablename = f'rapo_temp_nma_{self.control.process_id}'
        select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
        ctas = sql.text(f'CREATE TABLE {tablename} AS \n{select}')
        logger.info(f'{self.c} Creating {tablename} with query \n{ctas}')
        conn.execute(ctas)
        logger.debug(f'{self.c} {tablename} created')

        table = db.table(tablename)
        logger.debug(f'{self.c} Mismatching done')
        return table

    def count_fetched(self):
        """Count fetched records in data source."""
        if self.control.engine == 'DB':
            table = self.control.table_fetched
            fetched = self._count_fetched_to_table(table)
        return fetched

    def count_fetched_a(self):
        """Count fetched records in data source A."""
        if self.control.engine == 'DB':
            table = self.control.table_fetched_a
            fetched_a = self._count_fetched_to_table(table)
        return fetched_a

    def count_fetched_b(self):
        """Count fetched records in data source B."""
        if self.control.engine == 'DB':
            table = self.control.table_fetched_b
            fetched_b = self._count_fetched_to_table(table)
        return fetched_b

    def count_errors(self):
        """Count found discrepancies for control with ANL type.

        Returns
        -------
        errors : int
            Number of found discrepancies.
        """
        logger.debug(f'{self.c} Counting errors...')
        if self.control.engine == 'DB':
            conn = db.connect()
            table = self.control.table_errors
            count = sql.select([sql.func.count()]).select_from(table)
            errors = conn.execute(count).scalar()
        logger.debug(f'{self.c} Errors counted')
        return errors

    def count_matched(self):
        """Count records that were matched.

        Returns
        -------
        matched : int
            Number of found discrepancies.
        """
        logger.debug(f'{self.c} Counting matched...')
        if self.control.engine == 'DB':
            conn = db.connect()
            table = self.control.table_matched
            count = sql.select([sql.func.count()]).select_from(table)
            matched = conn.execute(count).scalar()
        logger.debug(f'{self.c} Matched counted')
        return matched

    def count_mismatched(self):
        """Count records that were not matched.

        Returns
        -------
        mismatched : int
            Number of found discrepancies.
        """
        logger.debug(f'{self.c} Counting mismatched')
        if self.control.engine == 'DB':
            conn = db.connect()
            table = self.control.table_mismatched
            count = sql.select([sql.func.count()]).select_from(table)
            mismatched = conn.execute(count).scalar()
        logger.debug(f'{self.c} Mismatched counted')
        return mismatched

    def save_errors(self):
        """Save found discrepancies in RAPO_RES table."""
        logger.debug(f'{self.c} Start saving...')
        conn = db.connect()
        table = self.control.table_errors
        process_id = sql.literal(self.control.process_id)
        select = sql.select([*table.columns,
                             process_id.label('rapo_process_id')])
        table = self.prepare_table_output()
        insert = table.insert().from_select(table.columns, select)
        conn.execute(insert)
        logger.debug(f'{self.c} Saving done')
        pass

    def save_mismatched(self):
        """Save found mismatches in RAPO_RES table."""
        logger.debug(f'{self.c} Start saving...')
        conn = db.connect()
        table = self.control.table_mismatched
        process_id = sql.literal(self.control.process_id)
        select = sql.select([*table.columns,
                             process_id.label('rapo_process_id')])
        table = self.prepare_table_output()
        insert = table.insert().from_select(table.columns, select)
        conn.execute(insert)
        logger.debug(f'{self.c} Saving done')
        pass

    def prepare_table_output(self):
        """Check RAPO_RES table and create it at initial control run.

        Returns
        -------
        table : sqlalchemy.Table
            Object reflecting RAPO_RES table.
        """
        tablename = f'rapo_res_{self.control.name}'.lower()
        conn = db.connect()
        if conn.engine.has_table(tablename) is False:
            logger.debug(f'{self.c} Table {tablename} will be created')

            columns = []
            output_columns = self.control.output_columns
            if output_columns is None or len(output_columns) == 0:
                if self.c.config.source_name is not None:
                    columns.extend(self.c.table_source.columns)
                if self.c.config.source_name_a is not None:
                    columns.extend(self.c.table_source_a.columns)
                if self.c.config.source_name_b is not None:
                    columns.extend(self.c.table_source_b.columns)
            else:
                for output_column in output_columns:
                    name = output_column['column']
                    column_a = output_column['column_a']
                    column_b = output_column['column_b']
                    if column_a is not None or column_b is not None:
                        table_a = self.control.table_source_a
                        table_b = self.control.table_source_b
                        if column_a is not None and column_b is not None:
                            column_a = table_a.c[column_a]
                            column_b = table_b.c[column_b]
                            column = sql.func.coalesce(column_a, column_b)
                        elif column_a is not None:
                            column = table_a.c[column_a]
                        elif column_b is not None:
                            column = table_b.c[column_b]
                        column = column.label(name) if name else column
                    else:
                        table = self.control.table_source
                        column = table.c[name]
                    columns.append(column)

            process_id = sql.literal(self.control.process_id)
            columns = [*columns, process_id.label('rapo_process_id')]
            select = sql.select(columns)
            select = select.where(sql.literal(1) == sql.literal(0))
            select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
            ctas = f'CREATE TABLE {tablename} AS \n{select}'
            index = (f'CREATE INDEX {tablename}_rapo_process_id_ix '
                     f'ON {tablename}(rapo_process_id) COMPRESS')
            compress = (f'ALTER TABLE {tablename} '
                        'MOVE ROW STORE COMPRESS ADVANCED')
            logger.debug(f'{self.c} Creating table {tablename} '
                         f'with query \n{ctas};\n{index};\n{compress};')
            conn.execute(ctas)
            conn.execute(index)
            conn.execute(compress)
            logger.debug(f'{self.c} {tablename} created')
        table = db.table(tablename)
        return table

    def drop_temporary_tables(self):
        """Clean all temporary tables created during control execution."""
        logger.debug(f'{self.c} Dropping temporary tables...')
        for tablename in ('table_fetched', 'table_errors',
                          'table_matched', 'table_mismatched',
                          'table_fetched_a', 'table_errors_a',
                          'table_fetched_b', 'table_errors_b'):
            table = getattr(self.control, tablename)
            if table is not None:
                table.drop(db.engine)
        logger.debug(f'{self.c} Temporary tables dropped')
        pass

    def hook(self):
        """Execute database hook procedure."""
        logger.debug(f'{self.c} Executing hook procedure...')
        conn = db.connect()
        process_id = self.control.process_id
        stmt = sql.text(f'begin rapo_control_hook({process_id}); end;')
        conn.execute(stmt)
        logger.debug(f'{self.c} Hook procedure executed')
        pass

    def _fetch_records_to_table(self, select, tablename):
        logger.debug(f'{self.c} Start fetching...')
        conn = db.connect()
        select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
        ctas = sql.text(f'CREATE TABLE {tablename} AS \n{select}')
        logger.info(f'{self.c} Creating {tablename} with query \n{ctas}')
        conn.execute(ctas)
        logger.info(f'{self.c} {tablename} created')
        table = db.table(tablename)
        logger.debug(f'{self.c} Fetching done')
        return table

    def _count_fetched_to_table(self, table):
        logger.debug(f'{self.c} Counting fetched in {table}...')
        conn = db.connect()
        count = sql.select([sql.func.count()]).select_from(table)
        fetched = conn.execute(count).scalar()
        logger.debug(f'{self.c} Fetched in {table} counted')
        return fetched
