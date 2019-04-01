from abc import ABC, abstractmethod


class Filter(ABC):
    """
    Abstract base class for all Filters.
    """
    def __init__(self, params):
        self.params = params

    def get_params(self):
        return self.params

    def set_params(self, params):
        self.params = params

    @abstractmethod
    def run(self):
        pass


class File2FileFilter(Filter):
    """
    Abstract base class for filters that take an input file and produce an output file.
    It requires a params dictionary with at least an input_path and output_path
    properties.
    """
    def __init__(self, params):
        super(File2FileFilter, self).__init__(params)

    def set_input_path(self, input_path):
        self.params['input_path'] = input_path

    def set_output_path(self, output_path):
        self.params['output_path'] = output_path

    def get_input_path(self):
        return f'{self.params["input_path"]}'

    def get_output_path(self):
        return f'{self.params["output_path"]}'


class FilterChain(Filter):
    """
    A wrapper over a sequence of filters that will be run in order.
    The params in a FilterChain are for global settings, each Filter will have their own.
    Generally speaking, the global settings override the local ones.
    """
    def __init__(self, params, fs):
        super(FilterChain, self).__init__(params)
        self.fs = fs

    def get_filters(self):
        return self.fs

    def append_filter(self, f):
        self.fs.append(f)

    def run(self):
        """
        Runs the filters in order
        :return:
        """
        for f in self.fs:
            f.run()

class File2DBFilter(Filter):
    """
    Base class for filters that take an input file and write something to a DB.
    It requires a params dictionary with at least an input_path and output_db_host,
    output_db_port, output_db_user, output_db_password, output_db_database
    """

    def __init__(self, params):
        super(File2DBFilter, self).__init__(params)

    def set_input_path(self, input_path):
        self.params['input_path'] = input_path

    def get_input_path(self):
        return f'{self.params["input_path"]}'
