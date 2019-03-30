import luigi
import gdtc.filters.file2file as f2f
import gdtc.tasks.workflowbuilder as wfb

# Para probar esto he creado el DummyTarget, porque una tarea que no devuelve
# Algo en su output creo que nunca es considerada terminada por Luigi
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

class TestFile2File(f2f.File2FileFilter):
    def run(self):
        print(f'Transforming {self.getInputPath()} into {self.getOutputPath()}')
        # Si no se crea realmente el fichero, no habrá LocalTarget y la Task de Luigi no parecerá completa nunca
        try:
            open(self.getOutputPath(), 'x').close()
        except FileExistsError:
            pass


if __name__ == '__main__':
    # Solo puedo crear dos del mismo tipo en la misma secuencia si tienen parámetros
    # distintos, sino serían la misma y tendrían una dependencia circular
    a = wfb.create_task_chain(ts = [T1(foo="hola"), T1(foo="adiós"), T2(), T3(), T4()])
    luigi.build([a])

    b = wfb.make_require_many(T1(foo="bar"), [T1(foo="coco"), T1(foo="kiko"), T2()])
    luigi.build([b])

    f1 = TestFile2File(params={})
    f1.setInputPath("f1input")
    f1.setOutputPath("f1output")
    c = wfb.create_file_2_file_task(f1)
    luigi.build([c])

    f2 = TestFile2File(params={})
    f2.setInputPath("f2input")
    f2.setOutputPath("f2output")

    filterChain = f2f.FileFilterChain([f1,f2], "fileinp", "fileoup", params={})
    filterChain.run()

    d = wfb.filter_chain_2_task_chain(filterChain)
    luigi.build([d])


