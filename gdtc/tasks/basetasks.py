import luigi

class File2FileTask(luigi.Task):
    """
    Used by the workflowbuilder methods to create luigi Tasks that produce a local file as output.
    """
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)

    def run(self):
        print("DUMMY RUN(). MUST BE CHANGED DYNAMICALLY TO DO SOME REAL WORK")

    def output(self):
        return luigi.LocalTarget(self.output_path)