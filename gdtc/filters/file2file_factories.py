from filters.file2file import HDF2TIF, TIF2SQL


# Factory methods to create filters
# TODO: Decidir qué hacemos para la gestión de parámetros, especialmente en filtros compuestos donde los
# distintos filtros individuales pueden interpretar de manera distinta algunos de ellos etc.
# TODO: Decidir si usamos el estilo diccionario de parámetros en general, o si tiene sentido tener este tipo
# de métodos factoría para facilitar al menos los usos más comunes

def hdf2tif(layer_num, input_file_name="NOT_ASSIGNED_YET", reproject=False, srcSRS=None, dstSRS=None, cell_res=None):
    params = {}
    params['input_path'] = f'{input_file_name}.hdf'
    params['output_path'] = f'{input_file_name}.tif'
    params['layer_num'] = layer_num
    params['reproject'] = reproject
    params['dstSRS'] = dstSRS
    params['cell_res'] = cell_res
    params['srcSRS'] = srcSRS

    return HDF2TIF(params)


def tif2sql(coord_sys, table, db, file_name = "NOT_ASSIGNED_YET"):
    params = {}
    params['input_path'] = f'{file_name}.tif'
    params['output_path'] = f'{file_name}.sql'
    params['coord_sys'] = coord_sys
    params['table'] = table
    params['db'] = db

    return TIF2SQL(params)