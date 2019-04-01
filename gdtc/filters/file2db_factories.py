import filters.basefilters_factories
import filters.file2file_factories
from aux import db as gdtcdb
from filters import basefilters as basefilters
from filters.file2db import ExecSQLFile

def execsqlfile(host, port, user, password, db, sql_file_path=None):
    params = {}
    params = gdtcdb.add_output_db_params({}, host, port, user, password, db)
    params['input_path'] = sql_file_path
    return ExecSQLFile(params)

def hdf2db(input_file_name, layer_num, coord_sys, table, host, port, user, password, db, reproject=False, srcSRS=None,
           dstSRS=None, cell_res=None):

    # output_file_name can be None because it will be filled in by the chaining
    f1 = filters.file2file_factories.hdf2tif(layer_num, input_file_name, None, reproject, srcSRS, dstSRS, cell_res)
    f2 = filters.file2file_factories.tif2sql(coord_sys, table, db)
    f3 = execsqlfile(host, port, user, password, db)

    # The chain does not use the params so a {} is OK. But, in the future this may change...
    hdf2db_chain = filters.basefilters_factories.create_file_filter_chain({}, [f1, f2],
                                                                          first_input_path = input_file_name)
    hdf2db_chain = filters.basefilters_factories.append_filter_to_chain(hdf2db_chain, f3)
    return hdf2db_chain