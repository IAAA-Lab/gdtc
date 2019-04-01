import luigi
import unittest

import filters.basefilters as basefilters
import filters.file2db_factories
import filters.file2file as f2f
import filters.file2file_factories
import tasks.workflowbuilder as wfb
import filters.file2db as f2db

# TODO: hacer los tests con ficheros y bd reales, mientras tanto fallarán siempre

class TestGISWorkflows(unittest.TestCase):

    def test_hdf2sql(self):
        # We create 2 filters
        f1 = filters.file2file_factories.hdf2tif(layer_num=0, dstSRS="EPSG:4358")
        _,coord_sys = str(f1.get_params()['dstSRS']).split(':')
        f2 = filters.file2file_factories.tif2sql(coord_sys = coord_sys, table ="table_dummy", db ="db_dummy")

        # We chain them
        filterchain = basefilters.create_file_filter_chain(params={}, fs=[f1, f2], first_input_path="fichero_entrada",
                                                          last_output_path="fichero_salida")
        # We may simply run them in order
        filterchain.run()

        # We may create and run a luigi workflow instead
        workflow = wfb.filter_chain_2_task_chain(filterchain)
        luigi.build([workflow])

    def test_hdf2db(self):
        filterchain = filters.file2db_factories.hdf2db(input_file_name="algo.hdf", layer_num=0, coord_sys="", table="", db="", host="", port="", user="", password="")
        filterchain.run()
        # We may create and run a luigi workflow instead
        workflow = wfb.filter_chain_2_task_chain(filterchain)
        luigi.build([workflow])


if __name__ == '__main__':
    unittest.main()