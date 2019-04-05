import luigi
import unittest
import os

import filters.basefilters_factories
import filters.file2db_factories
import filters.file2file_factories
import filters.db2db
import tasks.workflowbuilder as wfb

class TestGISWorkflows(unittest.TestCase):

    BASEDIR = os.getenv('GDTC_BASEDIR') or '/input'

    def test_hdf2sql(self):
        # Define input / output path
        input_file = f'{self.BASEDIR}/input_files/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'
        output_file = f'{self.BASEDIR}/output_files/output_file.sql'

        # We create 2 filters
        f1 = filters.file2file_factories.hdf2tif(layer_num=0, dstSRS="EPSG:4358")
        _,coord_sys = str(f1.get_params()['dstSRS']).split(':')
        f2 = filters.file2file_factories.tif2sql(coord_sys = coord_sys, table ="table_dummy", db ="db_dummy")

        # We chain them
        filterchain = filters.basefilters_factories.create_filter_chain(params={}, fs=[f1, f2], first_input=input_file,
                                                                            last_output=output_file)

        # We may simply run them in order
        filterchain.run()

    def test_hdf2db(self):
        # Define input / output path
        input_file = f'{self.BASEDIR}/input_files/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'

        filterchain = filters.file2db_factories.hdf2db(input_file_name=input_file, layer_num=0, coord_sys="4358", table="georefs",
                                                       host="localhost", port=8432, user="postgres", password="geodatatoolchainps", db="postgres")
        filterchain.run()

    def test_row_filter(self):
        input_params = {
            "input_db_host": "localhost",
            "input_db_port": 8432,
            "input_db_database": "postgres",
            "input_db_user": "postgres",
            "input_db_password": "geodatatoolchainps",
            "db_table": "georefs",
            "where_clause": "rid=1"
        }

        f1 = filters.db2db.RowFilter(params=input_params)

        f1.run()


if __name__ == '__main__':
    unittest.main()