# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Auxiliary, database-related, classes and functions
import uuid

import psycopg2

class Db():
    """
    Db is a class that encapsulates the parameters of a PostgreSQL DB.
    """
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        """
        Get a psycopg2 connection object to the database where the table is.
        """

        with psycopg2.connect(
           port = self.port,
           host = self.host,
           database = self.database,
           user = self.user,
           password = self.password) as connection:
               connection.set_client_encoding('utf-8')
               self.connection = connection

        return self.connection

    def execute_query(self, sql):
        with self.connect().cursor() as cur:
            cur.execute(sql)
            self.connection.commit()

    def to_ogr_connection_string(self):
        # PostgreSQL specific. But, at least for now, Db class is PostgreSQL specific
        return f'PG: host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}'

# TODO: create an object for database stuff? maybe a dictionary that can be mixed with others?
def add_output_db_params(params, host, port, user, password, db):
    params['output_db_host'] = host
    params['output_db_port'] = port
    params['output_db_user'] = user
    params['output_db_password'] = password
    params['output_db_database'] = db
    return params


def get_random_table_name():
    """
    :return: A random table name
    """
    return str(uuid.uuid4())