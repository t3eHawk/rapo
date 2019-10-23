import sqlalchemy as sql

from .database import db
from .logger import logger


class Executor():
    def __init__(self, bind):
        self.__bind = bind
        pass

    @property
    def control(self):
        return self.__bind

    def fetch_source_x(self):
        logger.debug(f'{self.control} Fetching X records...')
        conn = db.connect()
        if self.control.engine == 'db':
            select = self.control.table_x.select()
            date = self.control.date_x
            if date is not None:
                column = self.control.table_x.c[date]

                date_from = self.control.date_from
                date_to = self.control.date_to
                datefmt = '%Y-%m-%d %H:%M:%S'

                date_from = date_from.strftime(datefmt)
                date_to = date_to.strftime(datefmt)
                datefmt = 'YYYY-MM-DD HH24:MI:SS'

                date_from = sql.func.to_date(date_from, datefmt)
                date_to = sql.func.to_date(date_to, datefmt)
                select = select.where(column.between(date_from, date_to))

            tablename = f'rapo_fetx_{self.control.process_id}'
            select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
            ctas = sql.text(f'CREATE TABLE {tablename} AS \n{select}')
            logger.info(f'{self.control} Creating {tablename} '
                        f'with query \n{ctas}')
            conn.execute(ctas)
            logger.info(f'{self.control} {tablename} created')
        logger.debug(f'{self.control} Source X fetched')
        table = db.table(tablename)
        return table

    def analyze(self):
        logger.debug(f'{self.control} Analyzing X...')
        conn = db.connect()
        table_fetx = self.control.table_fetx
        output_x = self.control.output_x
        if output_x is None or len(output_x) == 0:
            select = table_fetx.select()
        else:
            columns = [table_fetx.c[column] for column in output_x]
            select = sql.select(columns)

        texts = []
        errors = self.control.errors
        for error in errors:
            text = []
            connexion = error['connexion']
            if len(texts) > 0:
                text.append(connexion)
            column_x = error['column_x']
            column_x = str(table_fetx.c[column_x])
            text.append(column_x)
            relation = error['relation']
            text.append(relation)
            value = error['value']
            is_column = error['is_column']
            value = str(table_fetx.c[value]) if is_column is True else value
            text.append(value)
            text = ' '.join(text)
            texts.append(text)
        select = select.where(sql.text('\n'.join(texts)))

        tablename = f'rapo_errx_{self.control.process_id}'
        select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
        ctas = sql.text(f'CREATE TABLE {tablename} AS \n{select}')
        logger.info(f'{self.control} Creating {tablename} '
                    f'with query \n{ctas}')
        conn.execute(ctas)
        logger.debug(f'{self.control} {tablename} created')

        table = db.table(tablename)
        return table

    def save_x(self):
        logger.debug(f'{self.control} Saving X results...')
        conn = db.connect()
        table_errx = self.control.table_errx
        process_id = sql.literal(self.control.process_id)
        select = sql.select([*table_errx.columns,
                             process_id.label('rapo_process_id')])
        table = self.prepare_table_resx()
        insert = table.insert().from_select(table.columns, select)
        conn.execute(insert)
        logger.debug(f'{self.control} Results X saved')
        pass

    def prepare_table_resx(self):
        tablename = f'rapo_resx_{self.control.name}'.lower()
        conn = db.connect()
        if conn.engine.has_table(tablename) is False:
            logger.debug(f'{self.control} Table {tablename} will be created')
            table_x = self.control.table_x
            output_x = self.control.output_x
            process_id = sql.literal(self.control.process_id)
            if output_x is None:
                columns = table_x.columns
            else:
                columns = [table_x.c[column] for column in output_x]
            columns = [*columns, process_id.label('rapo_process_id')]
            select = sql.select(columns)
            select = select.where(sql.literal(1) == sql.literal(0))
            select = select.compile(bind=db.engine, compile_kwargs=db.ckwargs)
            ctas = f'CREATE TABLE {tablename} AS \n{select}'
            index = (f'CREATE INDEX {tablename}_rapo_process_id_ix '
                     f'ON {tablename}(rapo_process_id) COMPRESS')
            compress = (f'ALTER TABLE {tablename} '
                         'MOVE ROW STORE COMPRESS ADVANCED')
            logger.debug(f'{self.control} Creating table {tablename} '
                         f'with query \n{ctas};\n{index};\n{compress};')
            conn.execute(ctas)
            conn.execute(index)
            conn.execute(compress)
            logger.debug(f'{self.control} {tablename} created')
        table = db.table(tablename)
        return table

    def drop_temporary_tables(self):
        logger.debug(f'{self.control} Dropping temporary tables...')
        for tablename in ('table_fetx', 'table_feta', 'table_fetb',
                          'table_errx', 'table_erra', 'table_errb'):
            table = getattr(self.control, tablename)
            if table is not None:
                table.drop(db.engine)
        logger.debug(f'{self.control} Temporary tables dropped')
        pass

    def hook(self):
        logger.debug(f'{self.control} Executing hook procedure...')
        conn = db.connect()
        process_id = self.control.process_id
        stmt = sql.text(f'begin rapo_control_hook({process_id}); end;')
        conn.execute(stmt)
        logger.debug(f'{self.control} Hook procedure executed')
        pass
