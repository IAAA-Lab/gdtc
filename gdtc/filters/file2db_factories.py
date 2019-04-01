import filters.file2file_factories
from aux import db as gdtcdb
from filters import basefilters as basefilters
from filters.file2db import ExecSQL


def hdf2db(input_file_name, layer_num, coord_sys, table, db, host, port, user, password, reproject=False, srcSRS=None, dstSRS=None, cell_res=None):
    f1 = filters.file2file_factories.hdf2tif(layer_num, input_file_name, reproject, srcSRS, dstSRS, cell_res)
    f2 = filters.file2file_factories.tif2sql(coord_sys, table, db)
    params = gdtcdb.add_output_db_params({}, host, port, user, password, db)
    f3 = ExecSQL(params)
    # TODO: Which params go here? Think carefully...
    chain = basefilters.create_file_filter_chain(params, [f1, f2], input_file_name, "tempfile")
    chain = basefilters.append_filter_to_chain(chain, f3)
    return chain