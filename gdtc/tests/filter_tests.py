import unittest
import os

import luigi

import gdtc.filters.basefilters_factories
import gdtc.filters.file2db_factories
import gdtc.filters.file2file_factories
import gdtc.filters.db2db_factories
import gdtc.filters.db2db
import gdtc.filters.file2file
import gdtc.filters.file2db
import gdtc.tasks.workflowbuilder as wfb


class TestGISWorkflows(unittest.TestCase):

    # Remember to define GDTC_BASEDIR without a / at the end
    BASEDIR = os.getenv('GDTC_BASEDIR') or '/input'

    def test_hdf2sql(self):
        # Define input / output path
        input_file = f'{self.BASEDIR}/input_files/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'
        output_file = f'{self.BASEDIR}/output_files/output_file.sql'

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
        input_file = f'{self.BASEDIR}/input_files/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'

        filterchain = gdtc.filters.file2db_factories.hdf2db(input_file_name=input_file, layer_num=0, coord_sys="4358", table="georefs",
                                                       host="localhost", port=8432, user="postgres", password="geodatatoolchainps", db="postgres")
        filterchain.run()

    def test_filter_chain(self):
        input_file = f'{self.BASEDIR}/input_files/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'
        last_output = {
            "db_host": "localhost",
            "db_port": 8432,
            "db_database": "postgres",
            "db_user": "postgres",
            "db_password": "geodatatoolchainps",
            "db_table": "geodata"
        }
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
            "output_db_host": "localhost",
            "output_db_port": 8432,
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
        input_file = f'{self.BASEDIR}/input_files/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'
        last_output = {
            "db_host": "localhost",
            "db_port": 8432,
            "db_database": "postgres",
            "db_user": "postgres",
            "db_password": "geodatatoolchainps",
            "db_table": "geodata"
        }

        f1 = gdtc.filters.file2file_factories.hdf2tif(layer_num=0, dstSRS="EPSG:4358")
        # TODO: Think about tif2sql filter, which is file2file filter but should pass table parameter to the next
        # filter. Now it's not happenning because file2file filter output gives just a path.
        # May be we have to overwrite get_output() method
        f2 = gdtc.filters.file2file_factories.tif2sql(coord_sys = 4358, db ="postgres", table='gdtc_table')
        f3 = gdtc.filters.file2db_factories.execsqlfile(output_db_table='gdtc_table')
        f4 = gdtc.filters.db2db_factories.rowfilter(where_clause="rid=1")

        filter_chain = gdtc.filters.basefilters_factories.create_filter_chain({}, [f1, f2, f3, f4], first_input=input_file, last_output=last_output)
        filter_chain.run()
    
    def test_shp2db(self):
        output = {
            "db_host": "localhost",
            "db_port": 8432,
            "db_database": "postgres",
            "db_user": "postgres",
            "db_password": "geodatatoolchainps",
            "db_table": "shape2db_test"
        }
        params = {}
        params['input_path'] = f'{self.BASEDIR}/input_files/ne_110m_coastline.shp'
        f1 = gdtc.filters.file2db.SHP2DB(params)
        f1.set_output(output)
        f1.run()
    
    def test_csv2db(self):
        output = {
            "db_host": "localhost",
            "db_port": 8432,
            "db_database": "postgres",
            "db_user": "postgres",
            "db_password": "geodatatoolchainps",
            "db_table": "csv_test"
        }
        params = {}
        params['input_path'] = f'{self.BASEDIR}/input_files/INE_Provs_2018.csv'
        f1 = gdtc.filters.file2db.CSV2DB(params)
        f1.set_output(output)
        f1.run()

    def test_excel2db(self):
        output = {
            "db_host": "localhost",
            "db_port": 8432,
            "db_database": "postgres",
            "db_user": "postgres",
            "db_password": "geodatatoolchainps",
            "db_table": "excel_test"
        }
        params = {}
        params['input_path'] = f'{self.BASEDIR}/input_files/parque_2016_tablas_auxiliares_anuario.xlsx'
        f1 = gdtc.filters.file2db.Excel2DB(params)
        f1.set_output(output)
        f1.run(sheet_name="parque_MUN", header=2)


    def test_fixed_width_file2db(self):
        output = {
            "db_host": "localhost",
            "db_port": 8432,
            "db_database": "postgres",
            "db_user": "postgres",
            "db_password": "geodatatoolchainps",
            "db_table": "fwf_test"
        }
        params = {}
        params['input_path'] = f'{self.BASEDIR}/input_files/Nomdef2017.txt'
        f1 = gdtc.filters.file2db.FixedWidthFile2DB(params)
        f1.set_output(output)
        f1.run()

    def test_plot_map(self):
        params = {}
        params['input_path'] = f'{self.BASEDIR}/input_files/110m_physical/ne_110m_coastline.shp'
        params['output_path'] = f'{self.BASEDIR}/output_files/ne_110m_coastline.png'
        f1 = gdtc.filters.file2file.PlotMap(params)
        f1.run()

    def test_s3_bucket(self):
        f1 = gdtc.filters.file2file_factories.s3_bucket_2_file(bucket_name='gdtc', object_name='test_object.png', output_path=f'{self.BASEDIR}/output_files/test_object.png')
        f1.run()


if __name__ == '__main__':
    unittest.main()