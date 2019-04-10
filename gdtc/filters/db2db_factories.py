from filters.db2db import RowFilter


# Factory methods to create filters
# TODO: Params based on environment variables

def rowfilter(where_clause, output_db_table=None, db_host="localhost", db_port=8432, db_database="postgres", db_user="postgres", db_password="geodatatoolchainps"):

    params = {}
    params['where_clause'] = where_clause
    params['output_db_table'] = output_db_table
    params['input_db_host'] = db_host
    params['input_db_port'] = db_port
    params['input_db_database'] = db_database
    params['input_db_user'] = db_user
    params['input_db_password'] = db_password

    return RowFilter(params)

