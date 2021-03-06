# Note: this is preferred to e.g. "import ogr" as the latter will be deprecated soon
from osgeo import ogr
from osgeo import osr
from osgeo import gdal
import pandas as pd
import sqlalchemy
import logging
import psycopg2

import gdtc.aux.db as gdtcdb
import gdtc.aux.srs as gdtcsrs
import gdtc.filters.basefilters as basefilters

# TODO: Is ExecSQLFile a Files2DBsFilter? Depending on the definition of Filter, that is not clear
class ExecSQLFile(basefilters.Files2DBsFilter):
    def run(self):
        logging.debug(f' Executing ExecSQLFile filter with params: {self.params}')
        db = gdtcdb.db_factory(self.get_outputs()[0])
        with open(self.get_inputs()[0], "r") as file:
            sql = file.read()
            logging.debug(f' SQL file to execute in: {self.get_inputs()[0]}')
        try:
            db.execute_query(sql)
        except psycopg2.Error as e:
            msg = f' Error executing query: {e}'
            logging.error(msg)
            raise RuntimeError(msg)  # If we fail, run must end with an error
        finally:
            db.close_connection()

class SHP2DB(basefilters.Files2DBsFilter):
    def run(self):

        logging.debug(f' Executing SHP2DB filter with params: {self.params}')

        # In general, we always want errors as exceptions. Having to enable them by hand is a "Python Gotcha" in gdal
        # see: https://trac.osgeo.org/gdal/wiki/PythonGotchas
        gdal.UseExceptions()

        shp_driver = ogr.GetDriverByName("ESRI Shapefile")
        input_data_source = shp_driver.Open(self.get_inputs()[0], 0)
        input_layer = input_data_source.GetLayer()

        # Find out the geometry type of the input to use it in the output
        # I am assuming that the input has at least one feature, and that all features are of the same
        # type (with shapefiles this is always so)
        geometry_type = input_layer.GetGeomType()

        # Warning: the shp format advertises polygons (ogr.wkbPolygon) even when there are multipolygons, and that
        # would make the insertion in PostGIS fail, so we will have to check and "upgrade" them to multipolygons when
        # loading to postgis
        if geometry_type == ogr.wkbPolygon:
            geometry_type = ogr.wkbMultiPolygon

        # If they are defined, they must be a string that can be processed with the Srs class
        if 'input_srs' in self.params:
            input_srs_string = self.params['input_srs']
        else:
            input_srs_string = None

        if 'output_srs' in self.params:
            output_srs_string = self.params['output_srs']
        else:
            output_srs_string = None

        # If input_srs is given use that. Otherwise, take the SRS of the input dataset
        if input_srs_string != None:
            input_srs = osr.SpatialReference()
            input_srs.ImportFromEPSG(int(gdtcsrs.Srs(input_srs_string).as_epsg_number()))
        else:
            input_srs = input_layer.GetSpatialRef()

        # If output_srs is given use that. Otherwise, take the same SRS used for the input
        if output_srs_string != None:
            output_srs = osr.SpatialReference()
            output_srs.ImportFromEPSG(int(gdtcsrs.Srs(output_srs_string).as_epsg_number()))
        else:
            output_srs = input_srs

        db = gdtcdb.db_factory(self.get_outputs()[0])
        db_connection_string = db.to_ogr_connection_string()
        conn = ogr.Open(db_connection_string)
        # TODO: Consider a mode where OVERWRITE is not always YES?
        output_layer = conn.CreateLayer(self.get_outputs()[0]['db_table'], output_srs, geometry_type, ['OVERWRITE=YES'])

        # Create table fields from those in the shapefile
        input_layer_defn = input_layer.GetLayerDefn()
        for i in range(0, input_layer_defn.GetFieldCount()):
            field_defn = input_layer_defn.GetFieldDefn(i)
            output_layer.CreateField(field_defn)

        # Add features from the input layer to the database table (output layer)
        output_layer_defn = output_layer.GetLayerDefn()
        for input_feature in input_layer:
            # Create output Feature
            output_feature = ogr.Feature(output_layer_defn)

            # Add field values from input Layer
            for i in range(0, output_layer_defn.GetFieldCount()):
                output_feature.SetField(output_layer_defn.GetFieldDefn(i).GetNameRef(), input_feature.GetField(i))

            # Set geometry
            geom = input_feature.GetGeometryRef()
            # Upgrade to multipolygon if polygon
            if geom.GetGeometryType() == ogr.wkbPolygon:
                geom = ogr.ForceToMultiPolygon(geom)
            output_feature.SetGeometry(geom.Clone())
            # Add new feature to output Layer
            output_layer.CreateFeature(output_feature)
            output_feature = None

        # This is required to close, and for the output to save, the data sources
        input_data_source = None
        conn = None

class CSV2DB(basefilters.Files2DBsFilter):
    def run(self, **options):
        """
        Insert CSV file to DB.
        :param options: optionally, any params that `pandas.read_csv <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html>`_
        can take
        :return:
        """

        logging.debug(f' Executing CSV2DB filter with params: {self.params}')

        db = gdtcdb.db_factory(self.get_outputs()[0])
        sqlalchemy_engine = sqlalchemy.create_engine(db.to_sql_alchemy_engine_string())
        input_csv = pd.read_csv(self.get_inputs()[0], **options)
        input_csv.to_sql(self.get_outputs()[0]['db_table'], con=sqlalchemy_engine, if_exists='replace')

class Excel2DB(basefilters.Files2DBsFilter):
    def run(self, **options):
        """
        Insert Excel file to DB.
        :param options: optionally, any params that `pandas.read_excel <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html>`_
        can take
        :return:
        """

        logging.debug(f' Executing Excel2DB filter with params: {self.params}')

        db = gdtcdb.db_factory(self.get_outputs()[0])
        sqlalchemy_engine = sqlalchemy.create_engine(db.to_sql_alchemy_engine_string())
        input_excel = pd.read_excel(self.get_inputs()[0], **options)
        input_excel.to_sql(self.get_outputs()[0]['db_table'], con=sqlalchemy_engine, if_exists='replace')


class FixedWidthFile2DB(basefilters.Files2DBsFilter):
    def run(self, **options):
        """
        Insert Fixed Width File to DB.
        :param options: optionally, any params that `pandas.read_fwf <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_fwf.html>`_
        can take
        :return:
        """

        logging.debug(f' Executing FixedWidthFile2DB filter with params: {self.params}')

        db = gdtcdb.db_factory(self.get_outputs()[0])
        sqlalchemy_engine = sqlalchemy.create_engine(db.to_sql_alchemy_engine_string())
        input_fwf = pd.read_fwf(self.get_inputs()[0], **options)
        input_fwf.to_sql(self.get_outputs()[0]['db_table'], con=sqlalchemy_engine, if_exists='replace')