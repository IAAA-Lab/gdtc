import subprocess

from osgeo import gdal
import geopandas
import matplotlib.pyplot as plt
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou, BucketAlreadyExists)
from gdtc.aux import db, output_factory

from gdtc.filters.basefilters import FilterVector

class ClipRasterWithSHP(FilterVector):

    def postRun(self):
        sql = f'''
                CREATE TABLE IF NOT EXISTS {self.params["output_db_table"]} (clip raster);
                INSERT INTO clips (clip) VALUES ((
                    SELECT ST_Clip (rast, 
                        (SELECT {self.params["geom"]} FROM {self.params["shp_table"]} WHERE ogc_fid = {self.params["ogc_fid"]})
                    , true)
                    AS clip
                FROM "{self.params["hdf_table"]}"
                WHERE rid = {self.params["rid"]}))

                '''

        gdtcdb = db.Db(self.params["output_db_host"], self.params["output_db_port"], self.params["output_db_database"], self.params["output_db_user"], self.params["output_db_password"])
        gdtcdb.execute_query(sql)

    def get_output(self):
        return output_factory.generate_db_output_getter_template(self)

    def set_output(self, params):
        output_factory.generate_db_output_setter_template(self, params)