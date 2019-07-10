import subprocess
import logging

from osgeo import gdal
import geopandas
import matplotlib.pyplot as plt
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou, BucketAlreadyExists)

from gdtc.filters.basefilters import Files2FilesFilter

class HDF2TIF(Files2FilesFilter):
    def run(self):
        logging.debug(f' Executing HDF2TIF filter with params: {self.params}')

        # Load file and get layer
        hdf = gdal.Open(self.get_inputs()[0], gdal.GA_ReadOnly)
        src_ds = gdal.Open(hdf.GetSubDatasets()[int(self.params['layer_num'])][0], gdal.GA_ReadOnly)

        # Ojo con los tipos, asumimos que reproject es bool etc.
        if self.params['reproject']:
            warp_options = gdal.WarpOptions(srcSRS=self.params["srcSRS"],
                                            dstSRS=self.params["dstSRS"],
                                            xRes=self.params["cell_res"], yRes=self.params["cell_res"],
                                            errorThreshold=0,
                                            resampleAlg=gdal.GRA_Average,
                                            warpOptions=['SAMPLE_GRID=YES', 'SAMPLE_STEP=1000', 'SOURCE_EXTRA=1000'])
            gdal.Warp(self.get_outputs()[0], src_ds, options=warp_options)

        else:
            # Generate file in tif format
            layer_array = src_ds.ReadAsArray()
            out = gdal.GetDriverByName('GTiff').Create(self.get_outputs()[0], src_ds.RasterXSize, src_ds.RasterYSize, 1,
                                                       gdal.GDT_Byte, ['COMPRESS=LZW', 'TILED=YES'])
            out.SetGeoTransform(src_ds.GetGeoTransform())
            out.SetProjection(src_ds.GetProjection())
            out.GetRasterBand(1).WriteArray(layer_array)
            # Write file to disk
            out = None
        
        output = self.get_outputs()
        
        logging.debug(f' Returning from HDF2TIF with output: {output}')

        return output


class TIF2SQL(Files2FilesFilter):
    def run(self):

        logging.debug(f' Executing TIF2SQL Filter with params: {self.params}')

        # Generate sql file
        cmd = f'raster2pgsql -I -C -s {self.params["coord_sys"]} \"{self.get_inputs()[0]}\" -F -d {self.params["db_table"]} > \"{self.get_outputs()[0]}\"'
        
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            logging.error(f' Error executing raster2pgsql command: {e}')
        
        output = self.get_outputs()[0]

        logging.debug(f' Returning from TIF2SQL with output: {output}')
        
        return output

class PlotMap(Files2FilesFilter):
    def run(self):

        logging.debug(f' Executing PlotMap Filter with params: {self.params}')

        map = geopandas.read_file(self.get_inputs()[0])
        map.plot()
        plt.savefig(self.get_outputs()[0])

class S3Bucket2File(Files2FilesFilter):
    # TODO: Think how to use self.get_input() in this Filter.
    def run(self):

        logging.debug(f' Executing PlotS3Bucket2FileMap Filter with params: {self.params}')

        minioClient = Minio(self.params['endpoint'], access_key=self.params['access_key'], secret_key=self.params['secret_key'], secure=True)
        try:
            minioClient.fget_object(self.params['bucket_name'], self.params['object_name'], self.get_outputs()[0])
        except ResponseError as e:
            logging.error(f' Error accessing S3 bucket: {e}')