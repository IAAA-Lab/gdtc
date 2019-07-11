import gdtc.aux.db
import gdtc.filters.basefilters_factories
import gdtc.filters.file2file_factories
from gdtc.aux import db as gdtcdb
from gdtc.filters.file2db import ExecSQLFile, SHP2DB, CSV2DB

def execsqlfile(db, sql_file_path=None):
    """
    db must include at least a db_table, not only a db connection
    :param db:
    :param sql_file_path:
    :return:
    """
    result = ExecSQLFile()
    result.set_inputs(sql_file_path)
    result.set_outputs(db)
    return result


def hdf2db(layer_num, coord_sys, db, db_table, hdf_file_path,
           reproject=False, srcSRS=None,  dstSRS=None, cell_res=None):

    # output_file_name can be None because it will be filled in by the chaining
    f1 = gdtc.filters.file2file_factories.hdf2tif(layer_num, hdf_file_path, None, reproject, srcSRS, dstSRS, cell_res)

    if isinstance(db, gdtcdb.Db):
        db_database = db.database
        db_dict = db.to_params_dict()
    else:
        db_database = db['db_database']
        db_dict = db
    f2 = gdtc.filters.file2file_factories.tif2sql(coord_sys, db_table, db_database)
    f3 = execsqlfile(db_dict)
    f3.generate_random_outputs()

    # The chain does not use the params so a {} is OK. But, in the future this may change...
    hdf2db_chain = gdtc.filters.basefilters_factories.create_filter_chain({}, [f1, f2, f3],
                                                                          first_input = hdf_file_path,
                                                                          last_output=f3.get_outputs()[0])

    return hdf2db_chain

def shp2db(input_srs, output_srs, db, db_table='', shp_file_path=''):
    params = {}
    params['input_srs'] = input_srs
    params['output_srs'] = output_srs
    result = SHP2DB(params)
    if isinstance(db, gdtcdb.Db):
        result.set_outputs(db.to_params_dict().update({"db_table": db_table}))
    else:  # if not a Db object, then assume a proper dictionary
        db_upd = {**db}
        db_upd.update({"db_table": db_table})  # If I put this in the previous line ({**db}.update...) it doesn't work
        result.set_outputs(db_upd)
    return result


def csv2db(db, db_table='', csv_file_path=''):
    result = CSV2DB()
    result.set_inputs(csv_file_path)
    if isinstance(db, gdtcdb.Db):
        result.set_outputs(db.to_params_dict().update({"db_table": db_table}))
    else:  # if not a Db object, then assume a proper dictionary
        db_upd = {**db}
        db_upd.update({"db_table": db_table})  # If I put this in the previous line ({**db}.update...) it doesn't work
        result.set_outputs(db_upd)
    return result


