import aux.db as gdtcdb
import aux.srs as gdtcsrs
import filters.basefilters as basefilters
import ogr
import osr


class ExecSQLFile(basefilters.File2DBFilter):
    def run(self):
        db = gdtcdb.Db(*self.get_output_connection().values())

        with open(self.params['input_path'], "r") as file:
            sql = file.read()

        db.execute_query(sql)


class SHPtoDB(basefilters.File2DBFilter):
    # Looking at this: https://pcjericks.github.io/py-gdalogr-cookbook/vector_layers.html#create-a-postgis-table-from-wkt
    def run(self):
        db = gdtcdb.Db(*self.get_output().values())
        db_connection_string = db.to_ogr_connection_string()
        ogrds = ogr.Open(db_connection_string)

        # If they are not defined, OGR will try to guess
        # If they are definied, they must be a string that can be processed with the Srs class
        # (for now a 'EPSG:code')
        input_srs_string = self.params['input_srs']
        output_srs_string = self.params['output_srs']

        if input_srs_string != None:
            input_srs = osr.SpatialReference()
            input_srs.ImportFromEPSG(gdtcsrs.Srs(input_srs_string).as_epsg_number())

        if output_srs_string != None:
            output_srs = osr.SpatialReference()
            output_srs.ImportFromEPSG(gdtcsrs.Srs(output_srs_string).as_epsg_number())

        #output_table = ogrds.CreateLayer(table, output_srs, DATA_TYPE??, ['OVERWRITE=YES'])

        #latlon_etrs_89 = gdtcsrs.Srs('EPSG:4258') # Iberian Peninsula and Balearic Islands
        #latlon_wgs_84 = gdtcsrs.Srs('EPSG:4326') # Canary Islands. For proj.4 this is equivalent to the 4258, :-/


