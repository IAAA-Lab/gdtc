import luigi
import unittest
from filters.file2db import SHPtoDB
import gdtc.aux.db as gdtcdb
import tasks.workflowbuilder as wfb

class I2DATIWorkflowsTests(unittest.TestCase):
    def test_population(self):
        params = gdtcdb.add_output_db_params({}, 'localhost', 5432, user='postgres', password='mysecretpassword',
                                             db='posgres')
        params['output_db_table'] = gdtcdb.get_random_table_name()
        f1 = SHPtoDB(params)
        f1.set_input("""
        /home/rbejar/Nextcloud/Research_Notebook/Spatial_Information_Infrastructures_And_Geo_Data
        /Data/recintos_municipales_inspire_peninbal_etrs89/recintos_municipales_inspire_peninbal_etrs89.shp""")

        # latlon_etrs_89 = gdtcsrs.Srs('EPSG:4258') # Iberian Peninsula and Balearic Islands
        # latlon_wgs_84 = gdtcsrs.Srs('EPSG:4326') # Canary Islands. For proj.4 this is equivalent to the 4258, :-/

        f1.run()
