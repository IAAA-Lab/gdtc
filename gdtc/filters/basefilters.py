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

    @abstractmethod
    def get_inputs(self):
        pass

    @abstractmethod
    def set_inputs(self, inputs):
        pass

    @abstractmethod
    def get_outputs(self):
        pass

    @abstractmethod
    def set_outputs(self, outputs):
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
    def __init__(self, fs, params={}):
        super(FilterChain, self).__init__(params)
        self.fs = fs

    def get_filters(self):
        return self.fs

    def append_filter(self, f):
        self.fs.append(f)

    def get_inputs(self):
        return self.fs[0].get_inputs()

    def set_inputs(self, inputs):
        self.fs[0].set_inputs(inputs)

    def get_outputs(self):
        return self.fs[-1].get_outputs()

    def set_outputs(self, outputs):
        self.fs[-1].set_outputs(outputs)

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

    def __init__(self, fs, params={}):
        super(FilterVector, self).__init__(params)
        self.fs = fs
    
    def get_inputs(self):
        for f in self.fs:
            yield f.get_inputs()

    def get_outputs(self):
        pass

    def set_inputs(self, _input):
        i=0
        for f in self.fs:
            f.set_inputs(_input[i])
            i = i+1

    def set_outputs(self, outputs):
        pass

    def run(self):
        for f in self.fs:
            f.run()
        self.postRun()
    
    def postRun(self):
        pass


class Files2FilesFilter(Filter):
    """
    Abstract base class for filters that take 1 or more input files and produce 1 or more output files.

    Params:
    input_paths : list of paths (as strings)
    output_paths: list of paths (as strings)
    """

    def __init__(self, params={}):
        super(Files2FilesFilter, self).__init__(params)

    def set_inputs(self, inputs):
        if isinstance(inputs, str):
            self.params['input_paths'] = [inputs]
        else: # This way if input_ is not iterable, a TypeError will be raised
            self.params['input_paths'] = []
            for i in inputs:
                self.params['input_paths'].append(i)

    def set_outputs(self, outputs):
        if isinstance(outputs, str):
            self.params['output_paths'] = [outputs]
        else: # This way if output is not iterable, a TypeError will be raised
            self.params['output_paths'] = []
            for o in outputs:
                self.params['output_paths'].append(o)

    def get_inputs(self):
        return self.params["input_paths"]

    def get_outputs(self):
        return self.params["output_paths"]

    def generate_random_outputs(self, num=1):
        """
        Num outputs (1 by default) will be created with random paths (and returned).
        """
        self.params["output_paths"] = []
        for i in range(num):
            self.params["output_paths"].append(gdtc.aux.file.create_tmp_file())
        return self.params["output_paths"]

class Files2DBFilter(Filter):
    """
    Base class for filters that take an input file and write something to a DB.
    It requires a params dictionary with at least an input_path and output_db_host,
    output_db_port, output_db_user, output_db_password, output_db_database
    """

    def __init__(self, params={}):
        super(Files2DBFilter, self).__init__(params)

    def set_inputs(self, inputs):
        if isinstance(inputs, str):
            self.params['input_paths'] = [inputs]
        else:  # This way if input_ is not iterable, a TypeError will be raised
            self.params['input_paths'] = []
            for i in inputs:
                self.params['input_paths'].append(i)

    def get_inputs(self):
        return self.params["input_paths"]

    def set_outputs(self, outputs):
        self.params['output_db_host'] = outputs["db_host"]
        self.params['output_db_port'] = outputs["db_port"]
        self.params['output_db_database'] = outputs["db_database"]
        self.params['output_db_user'] = outputs["db_user"]
        self.params['output_db_password'] = outputs["db_password"]
        self.params['output_db_table'] = outputs["db_table"] if "db_table" in outputs else gdtc.aux.db.get_random_table_name()

    def get_outputs(self):
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


class DBs2DBsFilter(Filter):
    """
    Abstract base class for filters that take 1 or more input tables in dbs and produce 1 or more output tables in dbs.
    """
    def __init__(self, params={}):
        super(DBs2DBsFilter, self).__init__(params)

    def set_inputs(self, inputs):
        """
        _input must be an iterable of dictionaries with database parameters and a table name, such as those
        produced by the function gdtc.aux.db.create_db_and_table_dict()
        """
        self.params['input_dbs'] = inputs

    def set_outputs(self, outputs):
        """
        output must be an iterable of dictionaries with database parameters and optional table names, such as those
        produced by the function gdtc.aux.db.create_db_and_table_dict(). If the optional table name is not there,
        a random name will be created
        """
        self.params['output_dbs'] = outputs
        for o in self.params['output_dbs']:
            if 'db_table' not in o:
                o['db_table'] = gdtc.aux.db.get_random_table_name()

    def get_inputs(self):
        return self.params['input_dbs']

    def get_outputs(self):
        """
        If output_dbs is not in params, the first time you call this method the outputs will be created with
        random table names and the same DB parameters (host,port,database,user,pwd) as the first input.
        If params includes a property n_outputs, that number of outputs will be created (only one otherwise).
        """
        if "output_dbs" not in self.params:
            self.params["output_dbs"] = []
            if "n_outputs" in self.params:
                for i in range(self.params["n_outputs"]):
                    self.params["output_dbs"].append(self.__create_random_output())
            else:  # Only one output
                self.params["output_dbs"].append(self.__create_random_output())

        return self.params["output_dbs"]

    def get_output_connection(self):
        """
        Returns an iterable of dictionaries with database parameters (like the outputs but without the table names)
        """
        output = self.get_outputs() # It is an iterable with at least one output
        output_connection = []
        for o in output:
            output_connection.append({
                "db_host": o["db_host"],
                "db_port": o["db_port"],
                "db_database": o["db_database"],
                "db_user": o["db_user"],
                "db_password": o["db_password"],
            })
        return output_connection

    def __create_random_output(self):
        base_output = {**self.get_inputs()[0]} # Shallow copy of input params
        base_output['db_table'] = gdtc.aux.db.get_random_table_name()
        return base_output


class DB2DBFilter(DBs2DBsFilter):
    """
    A specialization of DBs2DBsFilter when there are exactly 1 input and 1 output.
    """
    def __init__(self, params={}):
        super(DB2DBFilter, self).__init__(params)

    def set_inputs(self, inputs):
        # Kept for backwards compatibility, should be removed some day
        self.__to_input_params(inputs)

        # Super version takes an iterable, not a dictionary
        super().set_inputs([inputs])

    def set_outputs(self, outputs):
        # Kept for backwards compatibility, should be removed some day
        self.__to_output_params(outputs)
        if 'db_table' not in outputs:
            self.params['output_db_table'] = gdtc.aux.db.get_random_table_name()

        # Super version takes an iterable, not a dictionary
        super().set_outputs([outputs])

    def get_inputs(self):
        return super().get_inputs()[0]

    def get_outputs(self):
        return super().get_outputs()[0]

    def get_output_connection(self):
        return super().get_output_connection()[0]

    def __to_input_params(self, dict):
        for k in dict:
            self.params[f'input_{k}'] = dict[k]

    def __to_output_params(self, dict):
        for k in dict:
            self.params[f'output_{k}'] = dict[k]


class DB2FileFilter(Filter):
    """
    Base class for filters that take something from a DB as an input and write something
    to a File. It requires a params dictionay with at least: input_db_host, input_db_port,
    input_db_database, input_db_user, input_db_password
    If no output path is set, a random output file name is generated
    """

    def __init__(self, params={}):
        super(DB2FileFilter, self).__init__(params)

    def set_inputs(self, inputs):
        self.params['input_db_host'] = inputs["db_host"]
        self.params['input_db_port'] = inputs["db_port"]
        self.params['input_db_database'] = inputs["db_database"]
        self.params['input_db_user'] = inputs["db_user"]
        self.params['input_db_password'] = inputs["db_password"]
        self.params['input_db_table'] = inputs["db_table"]
    
    def get_inputs(self):
        return {
            "db_host": self.params["input_db_host"],
            "db_port": self.params["input_db_port"],
            "db_database": self.params["input_db_database"],
            "db_user": self.params["input_db_user"],
            "db_password": self.params["input_db_password"],
            "db_table": self.params["input_db_table"]
        }

    def set_outputs(self, outputs):
        self.params['output_path'] = outputs

    def get_outputs(self):
        if "output_path" not in self.params:
            self.params["output_path"] = gdtc.aux.file.create_tmp_file()
        
        return f'{self.params["output_path"]}'

