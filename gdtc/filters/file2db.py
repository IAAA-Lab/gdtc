# Note: this is preferred to e.g. "import ogr" as the latter will be deprecated soon
from osgeo import ogr
from osgeo import osr
from osgeo import gdal
import pandas as pd
import sqlalchemy

import gdtc.aux.db as gdtcdb
import gdtc.aux.srs as gdtcsrs
import gdtc.filters.basefilters as basefilters


class ExecSQLFile(basefilters.File2DBFilter):
    def run(self):
        db = gdtcdb.Db(*self.get_output_connection().values())

        with open(self.params['input_path'], "r") as file:
            sql = file.read()

        db.execute_query(sql)

class SHP2DB(basefilters.File2DBFilter):
    def run(self):
        # In general, we always want errors as exceptions. Having to enable them by hand is a "Python Gotcha" in gdal
        # see: https://trac.osgeo.org/gdal/wiki/PythonGotchas
        gdal.UseExceptions()

        shp_driver = ogr.GetDriverByName("ESRI Shapefile")
        input_data_source = shp_driver.Open(self.get_input(), 0)
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
            input_srs.ImportFromEPSG(gdtcsrs.Srs(input_srs_string).as_epsg_number())
        else:
            input_srs = input_layer.GetSpatialRef()

        # If output_srs is given use that. Otherwise, take the same SRS used for the input
        if output_srs_string != None:
            output_srs = osr.SpatialReference()
            output_srs.ImportFromEPSG(gdtcsrs.Srs(output_srs_string).as_epsg_number())
        else:
            output_srs = input_srs

        db = gdtcdb.Db(*self.get_output_connection().values())
        db_connection_string = db.to_ogr_connection_string()
        conn = ogr.Open(db_connection_string)
        # TODO: Consider a mode where OVERWRITE is not always YES?
        output_layer = conn.CreateLayer(self.params['output_db_table'], output_srs, geometry_type, ['OVERWRITE=YES'])

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

class CSV2DB(basefilters.File2DBFilter):
    def run(self, **options):
        """
        :param options: optionally, any params that `pandas.read_csv <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html>`_
        can take
        :return:
        """
        db = gdtcdb.Db(*self.get_output_connection().values())
        sqlalchemy_engine = sqlalchemy.create_engine(db.to_sql_alchemy_engine_string())
        input_csv = pd.read_csv(self.get_input(), **options)
        input_csv.to_sql(self.params['output_db_table'], con=sqlalchemy_engine, if_exists='replace')