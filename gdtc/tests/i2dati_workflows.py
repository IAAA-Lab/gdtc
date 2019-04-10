import luigi
import unittest
import os

import filters.basefilters_factories
import filters.file2db_factories
import filters.file2file_factories
import filters.db2db_factories
import filters.db2db
import tasks.workflowbuilder as wfb

class I2DATIWorkflowsTests(unittest.TestCase):
    def test_population(self):



        # This creates a new Task, that subclasses File2FileTask with some additional parameters
        f3 = TestFile2File(params={'input_path': aux.file.create_tmp_file(), 'coco': 24, 'foobar': 'minion'})
        f3.set_output_path(aux.file.create_tmp_file())
        ATaskClass = wfb.create_file_2_file_task_subclass(f3)
        # As I have created a new Task class, I can provide new values for the parameters before
        # runnning it (luigi style parameters, I could set them from the command line for instance)
        e = ATaskClass(input_path=f3.get_input_path(), output_path=f3.get_output_path(), coco="25", foobar='patata')
        self.assertEqual(e.coco, '25')
        self.assertEqual(e.foobar, 'patata')
        self.assertTrue(luigi.build([e]))
