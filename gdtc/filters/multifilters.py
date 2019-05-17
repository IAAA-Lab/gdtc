import subprocess
import psycopg2
from psycopg2 import sql
import logging

from osgeo import gdal
import geopandas
import matplotlib.pyplot as plt
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou, BucketAlreadyExists)
from gdtc.aux import db, output_factory

from gdtc.filters.basefilters import FilterVector

class ClipRasterWithSHP(FilterVector):

    def postRun(self):
        query = sql.Composed([sql.SQL("CREATE TABLE IF NOT EXISTS "),
                        sql.Identifier(self.params["output_db_table"]),
                        sql.SQL(" (clip raster); INSERT INTO clips (clip) VALUES (( SELECT ST_Clip (rast, (SELECT "),
                        sql.Identifier(self.params["geom"]),
                        sql.SQL(" FROM "),
                        sql.Identifier(self.params["shp_table"]),
                        sql.SQL(" WHERE ogc_fid = "),
                        sql.SQL(self.params["ogc_fid"]),
                        sql.SQL("), true) AS clip FROM "),
                        sql.Identifier(self.params["hdf_table"]),
                        sql.SQL(" WHERE rid = "),
                        sql.SQL(self.params["rid"]),
                        sql.SQL(" ));")
                    ])

        gdtcdb = db.Db(self.params["output_db_host"], self.params["output_db_port"], self.params["output_db_database"], self.params["output_db_user"], self.params["output_db_password"])
        str_query = query.as_string(gdtcdb.get_connection())
        logging.debug(f' SQL to execute: {str_query}')
        gdtcdb.execute_query(str_query)

    def get_outputs(self):
        return output_factory.generate_db_output_getter_template(self)

    def set_outputs(self, params):
        output_factory.generate_db_output_setter_template(self, params)