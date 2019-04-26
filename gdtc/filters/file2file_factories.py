import gdtc.aux.db
import gdtc.aux.file
from gdtc.filters.file2file import HDF2TIF, TIF2SQL


# Factory methods to create filters
# TODO: Decidir si usamos el estilo diccionario de parametros en general, o si tiene sentido tener este tipo
# de métodos factoría para facilitar al menos los usos más comunes

def hdf2tif(layer_num, input_file_name=None, output_file_name=None, reproject=False, srcSRS=None, dstSRS=None, cell_res=None):
    """

    :param layer_num:
    :param input_file_name: If it is None, no file will be created. It will have to be filled in later (e.g. when chaining)
    :param output_file_name:  If it is None, a temporary output file will be created
    :param reproject:
    :param srcSRS:
    :param dstSRS:
    :param cell_res:
    :return:
    """
    params = {}
    params['input_path'] = input_file_name
    params['output_path'] = gdtc.aux.file.create_tmp_file() if output_file_name is None else output_file_name
    params['layer_num'] = layer_num
    params['reproject'] = reproject
    params['dstSRS'] = dstSRS
    params['cell_res'] = cell_res
    params['srcSRS'] = srcSRS

    return HDF2TIF(params)


def tif2sql(coord_sys, db, table = None, input_file_name = None, output_file_name = None):
    """

    :param coord_sys:
    :param table:
    :param db:
    :param input_file_name: If it is None, no file will be created. It will have to be filled in later (e.g. when chaining)
    :param output_file_name:  If it is None, a temporary output file will be created
    :return:
    """
    params = {}
    params['input_path'] = input_file_name
    params['output_path'] = gdtc.aux.file.create_tmp_file() if output_file_name is None else output_file_name
    params['coord_sys'] = coord_sys
    params['table'] = table if table is not None else gdtc.aux.db.get_random_table_name()
    params['db'] = db

    return TIF2SQL(params)