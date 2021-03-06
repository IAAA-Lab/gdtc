import unittest

import luigi

import gdtc.filters.basefilters
import gdtc.filters.basefilters_factories
import gdtc.aux.file
import gdtc.tasks.workflowbuilder as wfb

# TODO: This tests require that luigid is running, it would be nice to check this somehow
# TODO: luigi.build returns True if there were no scheduling errors, but tasks may have failed nonetheless and that
# is not tested
class TestWorkFlowBuilding(unittest.TestCase):

    def test_task_chain(self):
        # Solo puedo crear dos del mismo tipo en la misma secuencia si tienen parámetros
        # distintos, sino serían la misma y tendrían una dependencia circular
        a = wfb.create_task_chain(ts=[T1(foo="hola"), T1(foo="adiós"), T2(), T3(), T4()])
        self.assertTrue(luigi.build([a]))

    def test_task_chain2(self):
        a = wfb.make_require(T1(foo="bar"), [T1(foo="coco"), T1(foo="kiko"), T2()])
        self.assertTrue(luigi.build([a]))

    def test_file_2_file_task(self):
        f1 = TestFile2File(params={})
        f1.set_input(gdtc.aux.file.create_tmp_file())
        f1.set_output(gdtc.aux.file.create_tmp_file())
        c = wfb.create_file_2_file_task(f1)
        self.assertTrue(luigi.build([c]))

    def test_filter_chain(self):
        f1 = TestFile2File(params={})
        f1.set_input(gdtc.aux.file.create_tmp_file())
        f1.set_output(gdtc.aux.file.create_tmp_file())
        f2 = TestFile2File(params={})
        f2.set_input(gdtc.aux.file.create_tmp_file())
        f2.set_output(gdtc.aux.file.create_tmp_file())
        filterChain = gdtc.filters.basefilters_factories.create_filter_chain(params={}, fs=[f1, f2],
                                                                             first_input=f1.get_input())
        # The test will also fail if this run fails (i.e. raises an Exception)
        filterChain.run()

        d = wfb.filter_chain_2_task_chain(filterChain)
        self.assertTrue(luigi.build([d]))

    def test_create_task_class(self):
        # This creates a new Task, that subclasses File2FileTask with some additional parameters
        f3 = TestFile2File(params={'input_path': gdtc.aux.file.create_tmp_file(), 'coco': 24, 'foobar': 'minion'})
        f3.set_output(gdtc.aux.file.create_tmp_file())
        ATaskClass = wfb.create_file_2_file_task_subclass(f3)
        # As I have created a new Task class, I can provide new values for the parameters before
        # runnning it (luigi style parameters, I could set them from the command line for instance)
        e = ATaskClass(input_path=f3.get_input(), output_path=f3.get_output(), coco="25", foobar='patata')
        self.assertEqual(e.coco, '25')
        self.assertEqual(e.foobar, 'patata')
        self.assertTrue(luigi.build([e]))

if __name__ == '__main__':
    unittest.main()



# Helper classes for these tests

# There is a Mock Target in luigi.mock, maybe that could be used instead
class DummyTarget(luigi.Target):
    def __init__(self, done):
        self.done = done

    def exists(self):
        return self.done

class T1(luigi.Task):
    foo = luigi.Parameter()

    # Sin un init como este, los Parameter de luigi no funcionarían
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.done = False

    def run(self):
        print(f"task 1 {self.foo}")
        self.done=True

    def output(self):
        return DummyTarget(self.done)

class T2(luigi.Task):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.done = False

    def run(self):
        print("task 2")
        self.done = True

    def output(self):
        return DummyTarget(self.done)

class T3(luigi.Task):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.done = False

    def run(self):
        print("task3")
        self.done = True

    def output(self):
        return DummyTarget(self.done)

class T4(luigi.Task):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.done = False

    def run(self):
        print("task4")
        self.done=True

    def output(self):
        return DummyTarget(self.done)



class TestFile2File(gdtc.filters.basefilters.File2FileFilter):
    def run(self):
        print(f'Transforming {self.get_input()} into {self.get_output()}')
        print('With these additional parameters:')
        for k,v in self.params.items():
            print(f'{k},{v}')
        # Si no se crea realmente el fichero, no habrá LocalTarget y la Task de Luigi no parecerá completa nunca
        try:
            open(self.get_output(), 'x').close()
        except FileExistsError:
            pass
