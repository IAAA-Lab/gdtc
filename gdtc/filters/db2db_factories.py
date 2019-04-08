import aux.random_table_name as rand
from filters.db2db import RowFilter


# Factory methods to create filters
# TODO: Params based on environment variables

def rowfilter(db_table, where_clause, db_table_out=None, db_host="localhost", db_port=8432, db_database="postgres", db_user="postgres", db_password="geodatatoolchainps"):

    params = {}
    params['db_table'] = db_table
    params['where_clause'] = where_clause
    params['db_table_out'] = db_table_out if db_table_out is not None else rand.get_random_table_name()
    params['db_host'] = db_host
    params['db_port'] = db_port
    params['db_database'] = db_database
    params['db_user'] = db_user
    params['db_password'] = db_password

    return RowFilter(params)

