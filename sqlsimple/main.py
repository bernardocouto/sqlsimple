from dbutils.pooled_db import PooledDB

import psycopg2


class Configuration(object):

    __instance = None

    def __init__(self, configuration: dict = None):
        if configuration:
            self.__configuration = configuration
        self.print_sql = self.__configuration.pop('print_sql') if 'print_sql' in self.__configuration else False
        self.pool = PooledDB(psycopg2, **self.__configuration)

    @staticmethod
    def instance(configuration: dict = None):
        if Configuration.__instance is None:
            Configuration.__instance = Configuration(configuration=configuration)
        return Configuration.__instance


class Database(object):

    pass
