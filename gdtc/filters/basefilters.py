from abc import ABC, abstractmethod

import os
import gdtc.aux.db
import gdtc.aux.file

DEBUG = os.getenv("GDTC_DEBUG")

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

    # Those abstract methods are needed so we can create a chain of any type without specifying
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

class FilterVector(Filter):
    """
    A wrapper over a bunch of input filter that produces a user defined output. It's a N to N filter.
    The filter vector receives multiple filters to be executed in parallel as an input, executes them and then it runs
    a postRun method. This method should be overrided.
    The get/set output methods shoould be overrided in order to chain this vector properly.
    This lets the user define multiple outputs for the filter
    """

    def __init__(self, params, fs):
        super(FilterVector, self).__init__(params)
        self.fs = fs
    
    def get_input(self):
        for f in self.fs:
            yield f.get_input()

    def get_output(self):
        pass

    def set_input(self, _input):
        i=0
        for f in self.fs:
            f.set_input(_input[i])
            i = i+1

    def set_output(self, output):
        pass

    def run(self):
        for f in self.fs:
            f.run()
        self.postRun()
    
    def postRun(self):
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
        if "output_path" not in self.params:
            self.params["output_path"] = gdtc.aux.file.create_tmp_file()
        
        return f'{self.params["output_path"]}'
         


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
        self.params['output_db_host'] = output["db_host"]
        self.params['output_db_port'] = output["db_port"]
        self.params['output_db_database'] = output["db_database"]
        self.params['output_db_user'] = output["db_user"]
        self.params['output_db_password'] = output["db_password"]
        self.params['output_db_table'] = output["db_table"] if "db_table" in output else gdtc.aux.db.get_random_table_name()

    def get_output(self):
        return {
            "db_host": self.params["output_db_host"],
            "db_port": self.params["output_db_port"],
            "db_database": self.params["output_db_database"],
            "db_user": self.params["output_db_user"],
            "db_password": self.params["output_db_password"],
            "db_table": self.params["output_db_table"]
        }

    def get_output_connection(self):
        return {
            "db_host": self.params["output_db_host"] if "output_db_host" in self.params else self.params["input_db_host"],
            "db_port": self.params["output_db_port"] if "output_db_port" in self.params else self.params["input_db_port"],
            "db_database": self.params["output_db_database"] if "output_db_database" in self.params else self.params["input_db_database"],
            "db_user": self.params["output_db_user"] if "output_db_user" in self.params else self.params["input_db_user"],
            "db_password": self.params["output_db_password"] if "output_db_password" in self.params else self.params["input_db_password"]
        }

    
class DB2DBFilter(Filter):
    """
    Base class for filters that take something from a DB as an input and write something
    to a DB. It requires a params dictionay with at least: input_db_host, input_db_port,
    input_db_database, input_db_user, input_db_password
    If no output params are set, inputs are considered to be the same as outputs
    """

    def __init__(self, params):
        super(DB2DBFilter, self).__init__(params)

    def set_input(self, input_):
        self.params['input_db_host'] = input_["db_host"]
        self.params['input_db_port'] = input_["db_port"]
        self.params['input_db_database'] = input_["db_database"]
        self.params['input_db_database'] = input_["db_database"]
        self.params['input_db_user'] = input_["db_user"]
        self.params['input_db_table'] = input_["db_table"]

    def set_output(self, output):
        self.params['output_db_host'] = output["db_host"]
        self.params['output_db_port'] = output["db_port"]
        self.params['output_db_database'] = output["db_database"]
        self.params['output_db_user'] = output["db_user"]
        self.params['output_db_password'] = output["db_password"]
        self.params['output_db_table'] = output["db_table"] if "db_table" in output else gdtc.aux.db.get_random_table_name()

    def get_input(self):
        return {
            "db_host": self.params["input_db_host"],
            "db_port": self.params["input_db_port"],
            "db_database": self.params["input_db_database"],
            "db_user": self.params["input_db_user"],
            "db_password": self.params["input_db_password"],
            "db_table": self.params["input_db_table"],
        }

    def get_output(self):
        return {
            "db_host": self.params["output_db_host"] if "output_db_host" in self.params else self.params["input_db_host"],
            "db_port": self.params["output_db_port"] if "output_db_port" in self.params else self.params["input_db_port"],
            "db_database": self.params["output_db_database"] if "output_db_database" in self.params else self.params["input_db_database"],
            "db_user": self.params["output_db_user"] if "output_db_user" in self.params else self.params["input_db_user"],
            "db_password": self.params["output_db_password"] if "output_db_password" in self.params else self.params["input_db_password"],
            "db_table": self.params["output_db_table"] if "output_db_table" in self.params else self.params["input_db_table"]
        }

    def get_output_connection(self):
        return {
            "db_host": self.params["output_db_host"] if "output_db_host" in self.params else self.params["input_db_host"],
            "db_port": self.params["output_db_port"] if "output_db_port" in self.params else self.params["input_db_port"],
            "db_database": self.params["output_db_database"] if "output_db_database" in self.params else self.params["input_db_database"],
            "db_user": self.params["output_db_user"] if "output_db_user" in self.params else self.params["input_db_user"],
            "db_password": self.params["output_db_password"] if "output_db_password" in self.params else self.params["input_db_password"]
        }


class DB2FileFilter(Filter):
    """
    Base class for filters that take something from a DB as an input and write something
    to a File. It requires a params dictionay with at least: input_db_host, input_db_port,
    input_db_database, input_db_user, input_db_password
    If no output path is set, a random output file name is generated
    """

    def __init__(self, params):
        super(DB2FileFilter, self).__init__(params)

    def set_input(self, input_):
        self.params['input_db_host'] = input_["db_host"]
        self.params['input_db_port'] = input_["db_port"]
        self.params['input_db_database'] = input_["db_database"]
        self.params['input_db_user'] = input_["db_user"]
        self.params['input_db_password'] = input_["db_password"]
        self.params['input_db_table'] = input_["db_table"]
    
    def get_input(self):
        return {
            "db_host": self.params["input_db_host"],
            "db_port": self.params["input_db_port"],
            "db_database": self.params["input_db_database"],
            "db_user": self.params["input_db_user"],
            "db_password": self.params["input_db_password"],
            "db_table": self.params["input_db_table"]
        }

    def set_output(self, output):
        self.params['output_path'] = output

    def get_output(self):
        if "output_path" not in self.params:
            self.params["output_path"] = gdtc.aux.file.create_tmp_file()
        
        return f'{self.params["output_path"]}'

