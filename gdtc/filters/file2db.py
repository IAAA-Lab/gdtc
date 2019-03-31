import gdtc.aux.db as gdtcdb
import filters.basefilters as basefilters
import filters.file2file as f2f

class ExecSQL(basefilters.File2DBFilter):
    def run(self):
        db = gdtcdb.Db(self.params['output_db_host'],
                       self.params['output_db_port'],
                       self.params['output_db_user'],
                       self.params['output_db_password'],
                       self.params['output_db_database'])

        with open(self.params['input_path'], "r") as file:
            sql = file.read()

        db.execute_query(sql)
# TODO: create an object for database stuff? maybe a dictionary that can be mixed with others?
def add_output_db_params(params, host, port, user, password, db):
    params['output_db_host'] = host
    params['output_db_port'] = port
    params['output_db_user'] = user
    params['output_db_password'] = password
    params['output_db_database'] = db
    return params

def hdf2db(input_file_name, layer_num, coord_sys, table, db, host, port, user, password, reproject=False, srcSRS=None, dstSRS=None, cell_res=None):
    f1 = f2f.hdf2tif(layer_num, input_file_name, reproject, srcSRS, dstSRS, cell_res)
    f2 = f2f.tif2sql(coord_sys, table, db)
    params = add_output_db_params({}, host, port, user, password, db)
    f3 = ExecSQL(params)
    # TODO: Which params go here? Think carefully...
    chain = basefilters.create_file_filter_chain(params, [f1, f2], input_file_name, "tempfile")
    chain = basefilters.append_filter_to_chain(chain, f3)
    return chain

