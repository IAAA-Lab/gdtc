import unittest
import os

import luigi

from gdtc.aux.config import Config as env
import gdtc.filters.basefilters_factories
import gdtc.filters.file2db_factories
import gdtc.filters.file2file_factories
import gdtc.filters.db2db_factories
import gdtc.filters.db2db
import gdtc.filters.file2file
import gdtc.filters.files2files
import gdtc.filters.file2db
import gdtc.filters.multifilters
import gdtc.tasks.workflowbuilder as wfb


class TestGISWorkflows(unittest.TestCase):

    def setUp(self):
        self.test_db = {
            "db_host": env.POSTGIS_HOST,
            "db_port": env.POSTGIS_PORT,
            "db_database": env.POSTGIS_DATABASE,
            "db_user": env.POSTGIS_USER,
            "db_password": env.POSTGIS_PASS,
        }

    def test_hdf2sql(self):
        # Define input / output path
        input_file = f'{env.GDTC_IN_VOL}/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'
        output_file = f'{env.GDTC_OUT_VOL}/output_file.sql'

        # We create 2 filters
        f1 = gdtc.filters.file2file_factories.hdf2tif(layer_num=0, dstSRS="EPSG:4358")
        _,coord_sys = str(f1.get_params()['dstSRS']).split(':')
        f2 = gdtc.filters.file2file_factories.tif2sql(coord_sys = coord_sys, table ="table_dummy", db ="db_dummy")

        # We chain them
        filterchain = gdtc.filters.basefilters_factories.create_filter_chain(params={}, fs=[f1, f2], first_input=input_file,
                                                                            last_output=output_file)

        # We may simply run them in order
        filterchain.run()

    def test_hdf2db(self):
        # Define input / output path
        input_file = f'{env.GDTC_IN_VOL}/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'

        filterchain = gdtc.filters.file2db_factories.hdf2db(input_file_name=input_file, layer_num=0, coord_sys="4358", table="georefs",
                                                       host=env.POSTGIS_HOST, port=env.POSTGIS_PORT, user="postgres", password="geodatatoolchainps", db="postgres")
        filterchain.run()

    def test_filter_chain(self):
        input_file = f'{env.GDTC_IN_VOL}/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'
        last_output = self.test_db
        last_output["db_table"] = "geodata"

        f1_params = {
            "layer_num": 0,
            "dstSRS": "EPSG:4358",
            "input_file_name": None,
            "output_file_name": None,
            "reproject": False,
            "srcSRS": None,
            "cell_res": None
        }
        f2_params = {
            "coord_sys": 4358, 
            "table": "geodata",
            "db": "postgres",
            "table": "gdtc_table"
        }
        f3_params = {
            "output_db_host": env.POSTGIS_HOST,
            "output_db_port": env.POSTGIS_PORT,
            "output_db_database": "postgres",
            "output_db_user": "postgres",
            "output_db_password": "geodatatoolchainps",
            "output_db_table": "gdtc_table"
        }
        f4_params = {
            "db_table": "geodata",
            "where_clause": "rid=1",
            "db_table_out": "geodata_2"
        }

        f1 = gdtc.filters.file2file.HDF2TIF(f1_params)
        f2 = gdtc.filters.file2file.TIF2SQL(f2_params)
        f3 = gdtc.filters.file2db.ExecSQLFile(f3_params)
        f4 = gdtc.filters.db2db.RowFilter(f4_params)

        filter_chain = gdtc.filters.basefilters_factories.create_filter_chain({}, [f1, f2, f3, f4], first_input=input_file, last_output=last_output)
        filter_chain.run()

    def test_db2db_factories(self):
        input_file = f'{env.GDTC_IN_VOL}/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'
        last_output = self.test_db
        last_output["db_table"] = "geodata"

        f1 = gdtc.filters.file2file_factories.hdf2tif(layer_num=0, dstSRS="EPSG:4358")
        # TODO: Think about tif2sql filter, which is file2file filter but should pass table parameter to the next
        # filter. Now it's not happenning because file2file filter output gives just a path.
        # May be we have to overwrite get_output() method
        f2 = gdtc.filters.file2file_factories.tif2sql(coord_sys = 4358, db ="postgres", table="gdtc_table")
        f3 = gdtc.filters.file2db_factories.execsqlfile(output_db_table="gdtc_table")
        f4 = gdtc.filters.db2db_factories.rowfilter(where_clause="rid=1")

        filter_chain = gdtc.filters.basefilters_factories.create_filter_chain({}, [f1, f2, f3, f4], first_input=input_file, last_output=last_output)
        filter_chain.run()
    
    def test_shp2db(self):
        output = self.test_db
        output["db_table"] = "shape2db_test"
        f1 = gdtc.filters.file2db.SHP2DB()
        f1.set_inputs(f'{env.GDTC_IN_VOL}/ne_110m_coastline.shp')
        f1.set_outputs(output)
        f1.run()
    
    def test_csv2db(self):
        output = self.test_db
        output["db_table"] = "csv_test"
        f1 = gdtc.filters.file2db.CSV2DB()
        f1.set_inputs(f'{env.GDTC_IN_VOL}/INE_Provs_2018.csv')
        f1.set_outputs(output)
        f1.run()

    def test_excel2db(self):
        output = self.test_db
        output["db_table"] = "excel_test"
        f1 = gdtc.filters.file2db.Excel2DB()
        f1.set_inputs(f'{env.GDTC_IN_VOL}/parque_2016_tablas_auxiliares_anuario.xlsx')
        f1.set_outputs(output)
        f1.run(sheet_name="parque_MUN", header=2)


    def test_fixed_width_file2db(self):
        output = self.test_db
        output["db_table"] = "fwf_test"
        f1 = gdtc.filters.file2db.FixedWidthFile2DB()
        f1.set_inputs(f'{env.GDTC_IN_VOL}/Nomdef2017.txt')
        f1.set_outputs(output)
        f1.run()

    def test_plot_map(self):
        f1 = gdtc.filters.file2file.PlotMap()
        f1.set_inputs(f'{env.GDTC_IN_VOL}/ne_110m_coastline.shp')
        f1.set_outputs(f'{env.GDTC_OUT_VOL}/ne_110m_coastline.png')
        f1.run()

    def test_s3_bucket(self):
        f1 = gdtc.filters.file2file_factories.s3_bucket_2_file(bucket_name='gdtc', object_name='test_object.png', output_path=f'{env.GDTC_OUT_VOL}/test_object.png')
        f1.run()

    def test_filter_vector(self):

        # Filter chain to insert HDF into db 
        input_path_hdf = f'{env.GDTC_IN_VOL}/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'
        last_output = self.test_db
        last_output["db_table"] = "geodata"

        f1 = gdtc.filters.file2file_factories.hdf2tif(layer_num=1, reproject=True, srcSRS='+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs', dstSRS="EPSG:4358", cell_res=1000)
        f2 = gdtc.filters.file2file_factories.tif2sql(coord_sys = 4358, db ="postgres", table='gdtc_table')
        f3 = gdtc.filters.file2db_factories.execsqlfile(output_db_table='gdtc_table')

        hdf2db_filter_chain = gdtc.filters.basefilters_factories.create_filter_chain({}, [f1, f2, f3], first_input=input_path_hdf, last_output=last_output)

        # Filter to insert SHP into db
        shp_params = {} 
        shp_params['input_paths'] = [f'{env.GDTC_IN_VOL}/Comunidades_Autonomas_ETRS89_30N.shp']
        shp_params['input_srs'] = 'EPSG:4358'
        shp_params['output_srs'] = 'EPSG:4358'
        shp_output = self.test_db
        shp_output["db_table"] = "comunidades_shp"

        f4 = gdtc.filters.file2db.SHP2DB(shp_params)
        f4.set_outputs(shp_output)

        # ClipRasterWithSHP is a filter vector

        params = {
            "output_db_host": env.POSTGIS_HOST,
            "output_db_port": env.POSTGIS_PORT,
            "output_db_database": "postgres",
            "output_db_user": "postgres",
            "output_db_password": "geodatatoolchainps",
            "output_db_table": "clips"
        }
        params["geom"] = "wkb_geometry"
        params["shp_table"] = "comunidades_shp"
        params["hdf_table"] = "gdtc_table"
        params["ogc_fid"] = "2"
        params["rid"] = "1"

        filter_vector = gdtc.filters.multifilters.ClipRasterWithSHP([hdf2db_filter_chain, f4], params)
        filter_vector.run()

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