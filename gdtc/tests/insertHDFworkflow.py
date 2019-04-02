import luigi
import unittest

import filters.basefilters_factories
import filters.file2db_factories
import filters.file2file_factories
import tasks.workflowbuilder as wfb

# TODO: hacer los tests con ficheros y bd reales, mientras tanto fallarán siempre

class TestGISWorkflows(unittest.TestCase):

    BASEDIR = '/home/victor/Documents/gdtc/gdtc/gdtc'

    def test_hdf2sql(self):
        # Define input / output path
        input_file = '{base_dir}/input_files/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'.format(base_dir=self.BASEDIR)
        output_file = '{base_dir}/output_files/output_file.sql'.format(base_dir=self.BASEDIR)

        # We create 2 filters
        f1 = filters.file2file_factories.hdf2tif(layer_num=0, dstSRS="EPSG:4358")
        _,coord_sys = str(f1.get_params()['dstSRS']).split(':')
        f2 = filters.file2file_factories.tif2sql(coord_sys = coord_sys, table ="table_dummy", db ="db_dummy")

        # We chain them
        filterchain = filters.basefilters_factories.create_file_filter_chain(params={}, fs=[f1, f2], first_input_path=input_file,
                                                                             last_output_path=output_file)
        # We may simply run them in order
        filterchain.run()

        # We may create and run a luigi workflow instead
        # workflow = wfb.filter_chain_2_task_chain(filterchain)
        # luigi.build([workflow])

    def test_hdf2db(self):
        # Define input / output path
        input_file = '{base_dir}/input_files/MCD12Q1.A2006001.h17v04.006.2018054121935.hdf'.format(base_dir=self.BASEDIR)

        filterchain = filters.file2db_factories.hdf2db(input_file_name=input_file, layer_num=0, coord_sys="4358", table="georefs",
                                                       host="localhost", port=8432, user="postgres", password="geodatatoolchainps", db="postgres")
        filterchain.run()
        # We may create and run a luigi workflow instead
        # workflow = wfb.filter_chain_2_task_chain(filterchain)
        # luigi.build([workflow])

if __name__ == '__main__':
    unittest.main()