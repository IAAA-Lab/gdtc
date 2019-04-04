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

    # Those abstract methods are needed so we can create a chain of any type without specify
    # what are we connecting
    @abstractmethod
    def get_input(self):
        pass

    @abstractmethod
    def set_input(self, input_):
        pass

    @abstractmethod
    def get_output(self):
        pass

    @abstractmethod
    def set_output(self, output):
        pass

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

    def set_input(self, input_):
        self.params['input_path'] = input_

    def set_output(self, output):
        self.params['output_path'] = output

    def get_input(self):
        return f'{self.params["input_path"]}'

    def get_output(self):
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

    def get_input(self):
        return f'{self.fs[0].get_input()}'

    def set_input(self, input_):
        self.fs[0].set_input(input_)

    def get_output(self):
        return self.fs[-1].get_output()

    def set_output(self, output):
        self.fs[-1].set_output(output)

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

    def set_input(self, input_):
        self.params['input_path'] = input_

    def get_input(self):
        return f'{self.params["input_path"]}'

    def set_output(self, output):
        self.params['output_db_host'] = output["output_db_host"]
        self.params['output_db_port'] = output["output_db_port"]
        self.params['output_db_database'] = output["output_db_database"]
        self.params['output_db_user'] = output["output_db_user"]
        self.params['output_db_password'] = output["output_db_password"]

    def get_output(self):
        # If no output is defined, input is considered as the output
        if self.params["output_db_host"] is None:
            return self.get_input()
        else:
            return {
                "output_db_host": self.params["output_db_host"],
                "output_db_port": self.params["output_db_port"],
                "output_db_database": self.params["output_db_database"],
                "output_db_user": self.params["output_db_user"],
                "output_db_password": self.params["output_db_password"]
            }

    

class DB2DBFilter(Filter):
    """
    Base class for filters that take something from a DB as an input and write something
    to a DB. It requires a params dictionay with at least: input_db_host, input_db_port,
    input_db_database, input_db_table, input_db_user, input_db_password
    If no output params are set, inputs are considered to be the same as outputs
    """

    def __init__(self, params):
        super(DB2DBFilter, self).__init__(params)

    def set_input(self, input_):
        self.params['input_db_host'] = input_["input_db_host"]
        self.params['input_db_port'] = input_["input_db_port"]
        self.params['input_db_database'] = input_["input_db_database"]
        self.params['input_db_user'] = input_["input_db_user"]
        self.params['input_db_password'] = input_["input_db_password"]

    def set_output(self, output):
        self.params['output_db_host'] = output["output_db_host"]
        self.params['output_db_port'] = output["output_db_port"]
        self.params['output_db_database'] = output["output_db_database"]
        self.params['output_db_user'] = output["output_db_user"]
        self.params['output_db_password'] = output["output_db_password"]
    
    def get_input(self):
        return {
            "input_db_host": self.params["input_db_host"],
            "input_db_port": self.params["input_db_port"],
            "input_db_database": self.params["input_db_database"],
            "input_db_user": self.params["input_db_user"],
            "input_db_password": self.params["input_db_password"]
        }

    def get_output(self):
        # If no output is defined, input is considered as the output
        if self.params["output_db_host"] is None:
            return self.get_input()
        else:
            return {
                "output_db_host": self.params["output_db_host"],
                "output_db_port": self.params["output_db_port"],
                "output_db_database": self.params["output_db_database"],
                "output_db_user": self.params["output_db_user"],
                "output_db_password": self.params["output_db_password"]
            }

class DB2FileFilter(Filter):
    """
    Base class for filters that take something from a DB as an input and write something
    to a File. It requires a params dictionay with at least: input_db_host, input_db_port,
    input_db_database, input_db_table, input_db_user, input_db_password
    If no output path is set, a random output file name is generated
    """

    def __init__(self, params):
        super(DB2FileFilter, self).__init__(params)

    def set_input(self, input_):
        self.params['input_db_host'] = input_["input_db_host"]
        self.params['input_db_port'] = input_["input_db_port"]
        self.params['input_db_database'] = input_["input_db_database"]
        self.params['input_db_user'] = input_["input_db_user"]
        self.params['input_db_password'] = input_["input_db_password"]
    
    def get_input(self):
        return {
            "input_db_host": self.params["input_db_host"],
            "input_db_port": self.params["input_db_port"],
            "input_db_database": self.params["input_db_database"],
            "input_db_user": self.params["input_db_user"],
            "input_db_password": self.params["input_db_password"]
        }

    def set_output(self, output):
        self.params['output_path'] = output

    def get_output(self):
        return f'{self.params["output_path"]}'
