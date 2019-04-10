import aux.db
import filters.basefilters_factories
import filters.file2file_factories
from aux import db as gdtcdb
from filters.file2db import ExecSQLFile

def execsqlfile(output_db_table=None, db_host="localhost", db_port=8432, db_user="postgres", db_password="geodatatoolchainps", db_database="postgres", sql_file_path=None):
    params = {}
    params = gdtcdb.add_output_db_params({}, db_host, db_port, db_user, db_password, db_database)
    params['input_path'] = sql_file_path
    params['output_db_table'] = output_db_table if output_db_table is not None else gdtcdb.get_random_table_name()
    return ExecSQLFile(params)

def hdf2db(input_file_name, layer_num, coord_sys, table, host, port, user, password, db, reproject=False, srcSRS=None,
           dstSRS=None, cell_res=None):

    # output_file_name can be None because it will be filled in by the chaining
    f1 = filters.file2file_factories.hdf2tif(layer_num, input_file_name, None, reproject, srcSRS, dstSRS, cell_res)
    f2 = filters.file2file_factories.tif2sql(coord_sys, table, db)
    f3 = execsqlfile(db_host=host, db_port=port, db_user=user, db_password=password, db_database=db)

    # The chain does not use the params so a {} is OK. But, in the future this may change...
    hdf2db_chain = filters.basefilters_factories.create_filter_chain({}, [f1, f2, f3],
                                                                          first_input = input_file_name, last_output=f3.get_output_connection())

    return hdf2db_chain