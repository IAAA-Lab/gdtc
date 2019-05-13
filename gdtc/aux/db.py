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
        self.connection = None

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
        conn = self.connection if self.connection is not None else self.connect()

        with conn.cursor() as cur:
            cur.execute(sql)
            self.connection.commit()


    def to_ogr_connection_string(self):
        # PostgreSQL specific. But, at least for now, Db class is PostgreSQL specific
        return f'PG: host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}'

    def to_sql_alchemy_engine_string(self):
        return f"""postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"""

    def to_params_dict(self, prefix = ''):
        """
        :param type: A prefix that will be added to the names of the params. E.g. if prefix is input_
        the param db_host will be named input_db_host. Only input_, output_ and the empty prefix are allowed
        :return: a dictionary
        """
        assert prefix == '' or prefix == 'input_' or prefix == 'output_', "Prefix must be empty, input_ or output_"
        params = {}
        params[f'{prefix}db_host'] = self.host
        params[f'{prefix}db_port'] = self.port
        params[f'{prefix}db_user'] = self.user
        params[f'{prefix}db_password'] = self.password
        params[f'{prefix}db_database'] = self.database
        return params

def add_output_db_params(params, host, port, user, password, db):
    """
    Add entries output_db_host, output_db_port etc. to the dictionary params with the values of host, port etc.
    :return:
    """
    db = Db(host, port, db, user, password)
    new_params = db.to_params_dict(prefix='output_')
    # Merge new_params with params (new syntax from Python 3.5). If keys are repeated, values in new_params
    # will take precedence
    params = {**params, **new_params}
    return params

def get_random_table_name():
    """
    :return: A random table name
    """
    return str(uuid.uuid4())

def create_db_and_table_dict(host, port, user, password, db, table):
    db = Db(host, port, db, user, password)
    dict = db.to_params_dict(prefix='')
    dict['db_table'] = table
    return dict