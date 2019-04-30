import luigi
import unittest
import filters.file2db as gdtcf2db
import gdtc.aux.db as gdtcdb
import tasks.workflowbuilder as wfb

class I2DATIWorkflowsTests(unittest.TestCase):
    def setUp(self):
        self.params = gdtcdb.add_output_db_params({}, 'localhost', 5432, user='postgres', password='mysecretpassword',
                                             db='postgres')

    def test_population(self):
        self.params['output_db_table'] = gdtcdb.get_random_table_name()
        f1 = SHPtoDB(self.params)
        f1.set_input("/home/rbejar/Nextcloud/Research_Notebook/Spatial_Information_Infrastructures_And_Geo_Data/Data/"
                     "recintos_municipales_inspire_peninbal_etrs89/recintos_municipales_inspire_peninbal_etrs89.shp")

        f1.run()

    def test_roads(self):
        # These are some 220k rows. This filter takes some 2 minutes to run in my computer.
        self.params['output_db_table'] = gdtcdb.get_random_table_name()
        f2 = gdtcf2db.SHP2DB(self.params)
        f2.set_input("/home/rbejar/Nextcloud/Research_Notebook/Spatial_Information_Infrastructures_And_Geo_Data/Data/"
                     "RT_ZARAGOZA/RT_VIARIA/rt_tramo_vial.shp")
        f2.run()

    def test_ine_provs(self):
        self.params['output_db_table'] = gdtcdb.get_random_table_name()
        f3 = gdtcf2db.CSV2DB(self.params)
        f3.set_input("/home/rbejar/Nextcloud/Research_Notebook/Spatial_Information_Infrastructures_And_Geo_Data/Data/"
                     "INE_Provs_2018.csv")
        f3.run()

        # We can pass pandas.to_csv parameters (this is a silly test to check that it does not fail)
        f3.run(names=['col1, col2'])
        # This is unnecessary, but it also works
        f3.run(encoding='utf-8')
