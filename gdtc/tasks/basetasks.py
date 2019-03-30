import luigi

class File2FileTask(luigi.Task):
    """
    Used by the workflowbuilder methods to create luigi Tasks that produce a local file as output.
    """
    inputPath = luigi.Parameter()
    outputPath = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)

    def run(self):
        print("TEST RUN(). MUST BE CHANGED DYNAMICALLY TO DO SOME REAL WORK")

    def output(self):
        return luigi.LocalTarget(self.outputPath)