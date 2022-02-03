from sqlsimple import Configuration, Database

import os
import unittest

CONFIGURATION = Configuration.instance(
    configuration_dict={
        'dbname': 'sqlsimple',
        'host': 'localhost',
        'maxconnections': 5,
        'password': 'sqlsimple',
        'port': 5432,
        'print_sql': True,
        'user': 'sqlsimple'
    },
    queries_directory=os.path.realpath(os.path.curdir) + '/queries/',
    migrations_directory=os.path.realpath(os.path.curdir) + '/migrations/'
)


class TestDefault(
    unittest.TestCase
):

    database = None

    @classmethod
    def setUpClass(
        cls
    ):
        cls.database = Database(CONFIGURATION)
        with cls.database as connection:
            (
                connection
                .execute(
                    '''
                    create table if not exists tests (
                        id bigserial not null,
                        name varchar(100),
                        description varchar(255),
                        constraint test_primary_key primary key (id)
                    )
                    '''
                )
            )

    @classmethod
    def tearDownClass(
        cls
    ):
        cls.database = Database(CONFIGURATION)
        with cls.database as connection:
            (
                connection
                .execute(
                    '''
                    drop table if exists tests
                    '''
                )
            )

    def setUp(
        self
    ):
        self.database = Database(CONFIGURATION)

    def test_find_all(
        self
    ):
        with self.database as connection:
            result = (
                connection
                .select('tests')
                .fields('id', 'name', 'description')
                .execute()
                .fetch_all()
            )
            self.assertEqual(len(result), 0)
            self.assertEqual(type(result), list)

    def test_find_all_with_file(
        self
    ):
        with self.database as connection:
            result = (
                connection
                .execute('tests.find_all.sql', skip_load_query=False)
                .fetch_all()
            )
            self.assertEqual(len(result), 0)
            self.assertEqual(type(result), list)

    def test_find_by_id(
        self
    ):
        with self.database as connection:
            result = (
                connection
                .select('tests')
                .fields('id', 'name', 'description')
                .where('id', 1)
                .execute()
                .fetch_one()
            )
            self.assertEqual(result, None)

    def test_find_by_id_with_file(
        self
    ):
        with self.database as connection:
            result = (
                connection
                .execute('tests.find_by_id.sql', parameters={'id': 1}, skip_load_query=False)
                .fetch_one()
            )
            self.assertEqual(result, None)
