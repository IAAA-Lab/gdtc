import unittest
import os

import luigi

from gdtc.aux.config import Config as env
from gdtc.aux.db import Db, add_table_to_db_dict
import gdtc.filters.basefilters_factories
import gdtc.filters.file2db_factories
import gdtc.filters.file2file_factories
import gdtc.filters.db2db_factories
import gdtc.filters.db2db
import gdtc.filters.file2file
import gdtc.filters.files2files
import gdtc.filters.file2db
import gdtc.tasks.workflowbuilder as wfb


class TestGISWorkflows(unittest.TestCase):

    def setUp(self):
        self.test_db = Db(env.POSTGIS_HOST, env.POSTGIS_PORT, env.POSTGIS_DATABASE,  env.POSTGIS_USER, env.POSTGIS_PASS)

    def test_hdf2sql(self):
        # Define input / output path
        input_file = f'{env.GDTC_IN_VOL}/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'
        output_file = f'{env.GDTC_OUT_VOL}/output_file.sql'

        # We create 2 filters
        f1 = gdtc.filters.file2file_factories.hdf2tif(layer_num=0, dstSRS="EPSG:4358")
        _,coord_sys = str(f1.get_params()['dstSRS']).split(':')
        f2 = gdtc.filters.file2file_factories.tif2sql(coord_sys = coord_sys, db_table ="table_dummy")

        # We chain them
        filterchain = gdtc.filters.basefilters_factories.create_filter_chain(params={}, fs=[f1, f2], first_input=input_file,
                                                                            last_output=output_file)

        # We may simply run them in order
        filterchain.run()

    def test_hdf2db(self):
        # Define input / output path
        input_file = f'{env.GDTC_IN_VOL}/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'

        filterchain = gdtc.filters.file2db_factories.hdf2db(layer_num=0, coord_sys="4358", db=self.test_db,
                                                            db_table="georefs", hdf_file_path=input_file)
        filterchain.run()

    def test_db2db_factories(self):
        input_file = f'{env.GDTC_IN_VOL}/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'
        last_output = self.test_db.to_params_dict()
        last_output = add_table_to_db_dict(last_output, 'final_geodata')

        f1 = gdtc.filters.file2file_factories.hdf2tif(layer_num=0, dstSRS="EPSG:4358")
        f2 = gdtc.filters.file2file_factories.tif2sql(coord_sys=4358, db_table="tif_table")
        f3 = gdtc.filters.file2db_factories.execsqlfile(db=add_table_to_db_dict(self.test_db.to_params_dict(), "tif_table"))
        f4 = gdtc.filters.db2db_factories.rowfilter(where_clause="rid=1", db=self.test_db, input_db_table="tif_table")

        filter_chain = gdtc.filters.basefilters_factories.create_filter_chain({}, [f1, f2, f3, f4],
                                                                              first_input=input_file,
                                                                              last_output=last_output)
        filter_chain.run()

    def test_shp2db(self):
        output = add_table_to_db_dict(self.test_db.to_params_dict(), "shape2db_test")
        f1 = gdtc.filters.file2db.SHP2DB()
        f1.set_inputs(f'{env.GDTC_IN_VOL}/ne_110m_coastline.shp')
        f1.set_outputs(output)
        f1.run()

    def test_csv2db(self):
        output = add_table_to_db_dict(self.test_db.to_params_dict(), "csv_test")
        f1 = gdtc.filters.file2db.CSV2DB()
        f1.set_inputs(f'{env.GDTC_IN_VOL}/INE_Provs_2018.csv')
        f1.set_outputs(output)
        f1.run()

    def test_excel2db(self):
        output = add_table_to_db_dict(self.test_db.to_params_dict(), "excel_test")
        f1 = gdtc.filters.file2db.Excel2DB()
        f1.set_inputs(f'{env.GDTC_IN_VOL}/parque_2016_tablas_auxiliares_anuario.xlsx')
        f1.set_outputs(output)
        f1.run(sheet_name="parque_MUN", header=2)


    def test_fixed_width_file2db(self):
        output = add_table_to_db_dict(self.test_db.to_params_dict(), "fwf_test")
        f1 = gdtc.filters.file2db.FixedWidthFile2DB()
        f1.set_inputs(f'{env.GDTC_IN_VOL}/Nomdef2017.txt')
        f1.set_outputs(output)
        f1.run()

    def test_plot_map(self):
        f1 = gdtc.filters.file2file.PlotMap()
        f1.set_inputs(f'{env.GDTC_IN_VOL}/ne_110m_coastline.shp')
        f1.set_outputs(f'{env.GDTC_OUT_VOL}/ne_110m_coastline.png')
        f1.run()

    # def test_s3_bucket(self):
    #     f1 = gdtc.filters.file2file_factories.s3_bucket_2_file(bucket_name='gdtc', object_name='test_object.png', output_path=f'{env.GDTC_OUT_VOL}/test_object.png')
    #     f1.run()
    #
    def test_clip_raster_with_shp(self):

        # Filter chain to insert HDF into db
        input_path_hdf = f'{env.GDTC_IN_VOL}/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'
        last_output = add_table_to_db_dict(self.test_db.to_params_dict(), "IRRELEVANT")


        f1 = gdtc.filters.file2file_factories.hdf2tif(layer_num=0, reproject=True,
                                                      srcSRS='+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs',
                                                      dstSRS="EPSG:25830", cell_res=1000)
        f2 = gdtc.filters.file2file_factories.tif2sql(coord_sys = 25830, db_table='tif_table')
        f3 = gdtc.filters.file2db_factories.execsqlfile(db=add_table_to_db_dict(self.test_db.to_params_dict(), "IRRELEVANT"))

        hdf2db_filter_chain = gdtc.filters.basefilters_factories.create_filter_chain({}, [f1, f2, f3], first_input=input_path_hdf, last_output=last_output)

        # Filter to insert SHP into db
        shp_params = {}
        shp_params['input_paths'] = [f'{env.GDTC_IN_VOL}/Comunidades_Autonomas_ETRS89_30N.shp']
        shp_params['input_srs'] = 'EPSG:25830'
        shp_params['output_srs'] = 'EPSG:25830'
        shp_output = add_table_to_db_dict(self.test_db.to_params_dict(), "comunidades_shp")

        f4 = gdtc.filters.file2db.SHP2DB(shp_params)
        f4.set_outputs(shp_output)


        params = {}
        params["geom"] = "wkb_geometry"
        params["ogc_fid"] = "2"
        params["rid"] = "1"

        clipper_input_0 = add_table_to_db_dict(self.test_db.to_params_dict(), "comunidades_shp")
        clipper_input_1 = add_table_to_db_dict(self.test_db.to_params_dict(), "tif_table")
        clipper_output = add_table_to_db_dict(self.test_db.to_params_dict(), "final_result")

        clipper = gdtc.filters.db2db.ClipRasterWithSHP(params)
        clipper.set_inputs([clipper_input_0, clipper_input_1])
        clipper.set_outputs(clipper_output)

        hdf2db_filter_chain.run()
        f4.run()
        clipper.run()

    def test_mosaic_rasters(self):
        input = [f'{env.GDTC_IN_VOL}/zgz_orto/zgz_clip1.tif',
                 f'{env.GDTC_IN_VOL}/zgz_orto/zgz_clip2.tif',
                 f'{env.GDTC_IN_VOL}/zgz_orto/zgz_clip3.tif']

        f1 = gdtc.filters.files2files.MosaicRasters(params={})
        f1.set_inputs(input)
        f1.set_outputs([f'{env.GDTC_OUT_VOL}/zgz_mosaic.tif'])
        f1.run()


if __name__ == '__main__':
    unittest.main()
