import subprocess

from osgeo import gdal
import geopandas
import matplotlib.pyplot as plt
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou, BucketAlreadyExists)
from gdtc.aux import db

from gdtc.filters.basefilters import FilterVector

class ClipRasterWithSHP(FilterVector):

    def postRun(self):
        sql = f'''
                CREATE TABLE IF NOT EXISTS clips (clip raster);
                INSERT INTO clips (clip) VALUES ((
                    SELECT ST_Clip (rast, 
                        (SELECT {self.params["geom"]} FROM {self.params["shp_table"]} WHERE ogc_fid = {self.params["ogc_fid"]})
                    , true)
                    AS clip
                FROM "{self.params["hdf_table"]}"
                WHERE rid = {self.params["rid"]}))

                '''

        gdtcdb = db.Db(self.params["db_host"], self.params["db_port"], self.params["db_database"], self.params["db_user"], self.params["db_password"])
        gdtcdb.execute_query(sql)