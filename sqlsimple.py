from dbutils.pooled_db import PooledDB
from typing import Any

import errno
import os
import psycopg2
import psycopg2.extras
import pystache

MIGRATIONS_DIRECTORY = os.path.realpath(os.path.curdir) + '/migrations/'
QUERIES_DIRECTORY = os.path.realpath(os.path.curdir) + '/queries/'


class Configuration(object):

    __instance__ = None

    def __init__(
        self,
        configuration_dict: dict = None,
        migrations_directory: str = MIGRATIONS_DIRECTORY,
        queries_directory: str = QUERIES_DIRECTORY
    ):
        if configuration_dict:
            self.configuration = configuration_dict
        self.migrations_directory = migrations_directory
        self.print_sql = self.configuration.pop('print_sql') if 'print_sql' in self.configuration else False
        self.pool = PooledDB(psycopg2, **self.configuration)
        self.queries_directory = queries_directory

    @staticmethod
    def instance(
        configuration_dict: dict = None,
        migrations_directory: str = MIGRATIONS_DIRECTORY,
        queries_directory: str = QUERIES_DIRECTORY
    ):
        if Configuration.__instance__ is None:
            Configuration.__instance__ = Configuration(configuration_dict, migrations_directory, queries_directory)
        return Configuration.__instance__


class ConfigurationInvalidException(
    Exception
):

    pass


class ConfigurationNotFoundException(
    Exception
):

    pass


class CursorWrapper(
    object
):

    def __init__(
        self,
        cursor: Any
    ):
        self.cursor = cursor

    def __iter__(
        self
    ):
        return self

    def __next__(
        self
    ):
        return self.next()

    def close(
        self
    ):
        self.cursor.close()

    def fetch_all(
        self
    ):
        return [DictWrapper(row) for row in self.cursor.fetchall()]

    def fetch_many(
        self,
        size: int
    ):
        return [DictWrapper(row) for row in self.cursor.fetchmany(size)]

    def fetch_one(
        self
    ):
        row = self.cursor.fetchone()
        if row is not None:
            return DictWrapper(row)
        else:
            self.close()
        return row

    def next(
        self
    ):
        row = self.fetch_one()
        if row is None:
            raise StopIteration()
        return row

    def row_count(
        self
    ):
        return self.cursor.rowcount


class Database(
    object
):

    def __enter__(
        self
    ):
        return self

    def __exit__(
        self,
        exception_type: Any,
        exception_value: Any,
        exception_traceback: Any
    ):
        if exception_type is None and exception_value is None and exception_traceback is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.disconnect()

    def __init__(
        self,
        configuration: Configuration = None
    ):
        self.configuration = Configuration.instance() if configuration is None else configuration
        self.connection = self.configuration.pool.connection()
        self.print_sql = self.configuration.print_sql
        self.queries_directory = self.configuration.queries_directory

    def delete(
        self,
        table: str
    ):
        return DeleteBuilder(self, table)

    def disconnect(
        self
    ):
        self.connection.close()

    def execute(
        self,
        sql: str,
        parameters: dict = None,
        skip_load_query: bool = True
    ):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if self.print_sql:
            result = f'Query: {sql} - Parameters: {parameters}'
            print(' '.join(result.replace('\n', '').strip().split()))
        if skip_load_query:
            sql = sql
        else:
            sql = self.load_query(sql, parameters)
            print(sql)
        cursor.execute(sql, parameters)
        return CursorWrapper(cursor)

    def insert(
        self,
        table: str
    ):
        return InsertBuilder(self, table)

    def update(
        self,
        table: str
    ):
        return UpdateBuilder(self, table)

    def load_query(
        self,
        query_name: str,
        parameters: dict = None
    ):
        query_name = query_name.replace('.sql', '')
        try:
            with open(self.queries_directory + query_name + '.sql') as file:
                query = file.read()
            if not parameters:
                return query
            else:
                return pystache.render(query, parameters)
        except IOError as exception:
            if exception.errno == errno.ENOENT:
                return query_name
            else:
                raise exception

    def paging(
        self,
        sql: str,
        page: int = 0,
        parameters: dict = None,
        size: int = 10,
        skip_load_query: bool = True
    ):
        if skip_load_query:
            sql = sql
        else:
            sql = self.load_query(sql, parameters)
        sql = '{} limit {} offset {}'.format(sql, size + 1, page * size)
        data = self.execute(sql, parameters, skip_load_query=True).fetch_all()
        last = len(data) <= size
        return Page(page, size, data[:-1] if not last else data, last)

    def select(
        self,
        table: str
    ):
        return SelectBuilder(self, table)


class DictWrapper(
    dict
):

    def __getattr__(
        self,
        item: Any
    ):
        if item in self:
            if isinstance(self[item], dict) and not isinstance(self[item], DictWrapper):
                self[item] = DictWrapper(self[item])
            return self[item]
        raise AttributeError('{} is not a valid attribute'.format(item))

    def __init__(
        self,
        data: Any
    ):
        super().__init__()
        self.update(data)

    def __setattr__(
        self,
        key: str,
        value: Any
    ):
        self[key] = value

    def as_dict(
        self
    ):
        return self


class Migration:

    def create_table_migration(
        self
    ):
        pass


class Page(
    dict
):

    def __init__(
        self,
        page_number: int,
        page_size: int,
        data,
        last
    ):
        super().__init__()
        self['data'] = self.data = data
        self['last'] = self.last = last
        self['page_number'] = self.page_number = page_number
        self['page_size'] = self.page_size = page_size


class SQLBuilder(
    object
):

    def __init__(
        self,
        database: Any,
        table: str
    ):
        self.database = database
        self.parameter = {}
        self.table = table
        self.where_conditions = []

    def execute(
        self
    ):
        return self.database.execute(self.sql(), self.parameter, True)

    def sql(
        self
    ):
        pass

    def where_all(
        self,
        data: Any
    ):
        for value in data.keys():
            self.where(value, data[value])
        return self

    def where_build(
        self
    ):
        if len(self.where_conditions) > 0:
            conditions = ' and '.join(self.where_conditions)
            return 'where {}'.format(conditions)
        else:
            return ''

    def where(
        self,
        field: str,
        value: Any,
        constant: bool = False,
        operator: str = '='
    ):
        if constant:
            self.where_conditions.append('{} {} {}'.format(field, operator, value))
        else:
            self.parameter[field] = value
            self.where_conditions.append('{0} {1} %({0})s'.format(field, operator))
        return self


class DeleteBuilder(
    SQLBuilder
):

    def sql(
        self
    ):
        return 'delete from {} {}'.format(self.table, self.where_build())


class InsertBuilder(
    SQLBuilder
):

    def __init__(
        self,
        database: Database,
        table: str
    ):
        super(InsertBuilder, self).__init__(database, table)
        self.constants = {}

    def set(
        self,
        field: str,
        value: Any,
        constant: bool = False
    ):
        if constant:
            self.constants[field] = value
        else:
            self.parameter[field] = value
        return self

    def set_all(
        self,
        data: Any
    ):
        for value in data.keys():
            self.set(value, data[value])
        return self

    def sql(
        self
    ):
        if len(set(list(self.parameter.keys()) + list(self.constants.keys()))) == len(self.parameter.keys()) + len(self.constants.keys()):
            columns = []
            values = []
            for field in self.constants:
                columns.append(field)
                values.append(self.constants[field])
            for field in self.parameter:
                columns.append(field)
                values.append('%({})s'.format(field))
            return 'insert into {} ({}) values ({}) returning *'.format(self.table, ', '.join(columns), ', '.join(values))
        else:
            raise ValueError('There are repeated keys in constants and values')


class SelectBuilder(
    SQLBuilder
):

    def __init__(
        self,
        database: Database,
        table: str
    ):
        super(SelectBuilder, self).__init__(database, table)
        self.select_fields = ['*']
        self.select_group_by = []
        self.select_order_by = []
        self.select_page = ''

    def fields(
        self,
        *fields: Any
    ):
        self.select_fields = fields
        return self

    def group_by(
        self,
        *fields: Any
    ):
        self.select_group_by = fields
        return self

    def order_by(
        self,
        *fields: Any
    ):
        self.select_order_by = fields
        return self

    def paging(
        self,
        page: int = 0,
        size: int = 10
    ):
        self.select_page = 'limit {} offset {}'.format(size + 1, page * size)
        data = self.execute().fetch_all()
        last = len(data) <= size
        return Page(page, size, data[:-1] if not last else data, last)

    def sql(
        self
    ):
        group_by = ', '.join(self.select_group_by)
        if group_by != '':
            group_by = 'group by {}'.format(group_by)
        order_by = ', '.join(self.select_order_by)
        if order_by != '':
            order_by = 'order by {}'.format(order_by)
        return 'select {} from {} {} {} {} {}'.format(
            ', '.join(self.select_fields),
            self.table,
            self.where_build(),
            group_by,
            order_by,
            self.select_page
        )


class UpdateBuilder(
    SQLBuilder
):

    def __init__(
        self,
        database: Database,
        table: str
    ):
        super(UpdateBuilder, self).__init__(database, table)
        self.statements = []

    def set(
        self,
        field: str,
        value: Any,
        constant: bool = False
    ):
        if constant:
            self.statements.append('{} = {}'.format(field, value))
        else:
            self.statements.append('{0} = %({0})s'.format(field))
            self.parameter[field] = value
        return self

    def set_all(
        self,
        data: Any
    ):
        for value in data.keys():
            self.set(value, data[value])
        return self

    def set_build(
        self
    ):
        if len(self.statements) > 0:
            statements = ', '.join(self.statements)
            return f'set {statements}'
        else:
            return ''

    def sql(
        self
    ):
        return f'update {self.table} {self.set_build()} {self.where_build()}'
