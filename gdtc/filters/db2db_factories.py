from gdtc.filters.db2db import RowFilter
import gdtc.aux.db as gdtcdb

def rowfilter(where_clause, db, input_db_table, output_db_table=None):

    params = {}
    params['where_clause'] = where_clause
    result = RowFilter(params)

    if isinstance(db, gdtcdb.Db):
        result.set_inputs(gdtcdb.add_table_to_db_dict(db.to_params_dict(), input_db_table))
    else:  # if not a Db object, then assume a db dictionary
        result.set_inputs(gdtcdb.add_table_to_db_dict({**db}, input_db_table))

    if output_db_table is not None:
        result.set_outputs(gdtcdb.add_table_to_db_dict({**db}, output_db_table))

    return result









