import gdtc.aux.db
import gdtc.aux.file
from gdtc.filters.file2file import HDF2TIF, TIF2SQL, S3Bucket2File
from gdtc.aux.config import Config as env
import os

# Factory methods to create filters

def hdf2tif(layer_num, input_file_name=None, output_file_name=None, reproject=False, srcSRS=None, dstSRS=None, cell_res=None):
    params = {}
    params['layer_num'] = layer_num
    params['reproject'] = reproject
    params['dstSRS'] = dstSRS
    params['cell_res'] = cell_res
    params['srcSRS'] = srcSRS

    result = HDF2TIF(params)
    result.set_inputs(input_file_name)
    result.set_outputs(output_file_name)
    return result


def tif2sql(coord_sys, db_table, input_file_name=None, output_file_name=None):
    params = {}
    params['coord_sys'] = coord_sys
    params['db_table'] = db_table

    result = TIF2SQL(params)
    result.set_inputs(input_file_name)
    result.set_outputs(output_file_name)
    return result


def s3_bucket_2_file(bucket_name, object_name, endpoint='s3.dualstack.eu-west-1.amazonaws.com', output_path=None):
    params = {}
    params['bucket_name'] = bucket_name
    params['object_name'] = object_name
    params['endpoint'] = endpoint
    params['access_key'] = env.GDTC_ACCESS_KEY
    params['secret_key'] = env.GDTC_SECRET_KEY

    result = S3Bucket2File(params)
    result.set_outputs(output_path)
    return result