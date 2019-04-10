import aux.db as gdtcdb
import aux.srs as gdtcsrs
import filters.basefilters as basefilters
# Note: this is preferred to e.g. "import ogr" as the latter will be deprecated soon
from osgeo import ogr
from osgeo import osr
from osgeo import gdal

class ExecSQLFile(basefilters.File2DBFilter):
    def run(self):
        db = gdtcdb.Db(*self.get_output_connection().values())

        with open(self.params['input_path'], "r") as file:
            sql = file.read()

        db.execute_query(sql)


class SHPtoDB(basefilters.File2DBFilter):
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
        #feature = input_layer.GetNextFeature()
        #geom = feature.GetGeometryRef()
        #geometry_type = geom.GetGeometryName()
        geometry_type = input_layer.GetGeomType()

        # If they are defined, they must be a string that can be processed with the Srs class
        input_srs_string = self.params['input_srs']
        output_srs_string = self.params['output_srs']

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

        db = gdtcdb.Db(*self.get_output().values())
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
            output_feature.SetGeometry(geom.Clone())
            # Add new feature to output Layer
            output_layer.CreateFeature(output_feature)
            output_feature = None

        # This is required to close, and for the output to save, the data sources
        input_data_source = None
        conn = None


