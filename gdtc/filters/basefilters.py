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
        self.params = {**params} # Shallow copy

    def get_params(self):
        return {**self.params} # Defensive shallow copy

    def set_params(self, params):
        self.params = {**params} # Shallow copy

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
        if inputs is not None:
            if isinstance(inputs, str):
                self.params['input_paths'] = [inputs]
            else: # This way if input_ is not iterable, a TypeError will be raised
                self.params['input_paths'] = []
                for i in inputs:
                    self.params['input_paths'].append(i)
        else:
            self.params['input_paths'] = []

    def set_outputs(self, outputs):
        if outputs is not None:
            if isinstance(outputs, str):
                self.params['output_paths'] = [outputs]
            else: # This way if output is not iterable, a TypeError will be raised
                self.params['output_paths'] = []
                for o in outputs:
                    self.params['output_paths'].append(o)
        else:
            self.params['output_paths'] = []

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

class Files2DBsFilter(Filter):
    """
    Abstract base class for filters that take 1 or more input files and write something to 1 or more DBs.

    Params:
    input_paths : list of paths (as strings)
    output_dbs: list of dbs (dictionaries as produced by gdtc.aux.db.create_db_and_table_dict()
    """

    def __init__(self, params={}):
        super(Files2DBsFilter, self).__init__(params)

    def set_inputs(self, inputs):
        if inputs is not None:
            if isinstance(inputs, str):
                self.params['input_paths'] = [inputs]
            else:  # This way if input_ is not iterable, a TypeError will be raised
                self.params['input_paths'] = []
                for i in inputs:
                    self.params['input_paths'].append(i)
        else:
            self.params['input_paths'] = []

    def set_outputs(self, outputs):
        if outputs is not None:
            if isinstance(outputs, dict):
                self.params['output_dbs'] = [{**outputs}]  # Shallow copy
            else:  # If input_ is not iterable, a TypeError will be raised
                self.params['output_dbs'] = []
                for o in outputs:
                    self.params['output_dbs'].append({**o})  # Shallow copy
        else:
            self.params['output_dbs'] = []

    def get_inputs(self):
        return self.params['input_paths']

    def get_outputs(self):
        return self.params['output_dbs']

    def generate_random_outputs(self):
        """
        For each db in output_dbs, a random table name will be assigned to it.
        The rest of the db parameters (host, port, etc.) will not be modified.
        """
        for o in self.params["output_dbs"]:
            o['db_table'] = gdtc.aux.db.create_random_table_name()
        return self.params["output_dbs"]



class DBs2DBsFilter(Filter):
    """
    Abstract base class for filters that take 1 or more input dbs and write something to 1 or more dbs.

    Params:
    input_paths : list of dbs (dictionaries as produced by gdtc.aux.db.create_db_and_table_dict()
    output_dbs: list of dbs (dictionaries as produced by gdtc.aux.db.create_db_and_table_dict()
    """
    def __init__(self, params={}):
        super(DBs2DBsFilter, self).__init__(params)

    def set_inputs(self, inputs):
        if inputs is not None:
            if isinstance(inputs, dict):
                self.params['input_dbs'] = [{**inputs}] # Shallow copy
            else:  # If input_ is not iterable, a TypeError will be raised
                self.params['input_dbs'] = []
                for i in inputs:
                    self.params['input_dbs'].append({**i})  # Shallow copy
        else:
            self.params['input_dbs'] = []

    def set_outputs(self, outputs):
        if outputs is not None:
            if isinstance(outputs, dict):
                self.params['output_dbs'] = [{**outputs}]  # Shallow copy
            else:  # If input_ is not iterable, a TypeError will be raised
                self.params['output_dbs'] = []
                for o in outputs:
                    self.params['output_dbs'].append({**o})  # Shallow copy
        else:
            self.params['output_dbs'] = []

    def get_inputs(self):
        return self.params['input_dbs']

    def get_outputs(self):
        return self.params["output_dbs"]

    def generate_random_outputs(self, num=1):
        """
        if output_dbs has not been assigned yet, it will create it with num random output parameters (all with the
        same db connection as the first input_db, but with random table names). If output_dbs exists, it will only
        assign random table names for all of them, keeping the db connection parameters as they are.
        """
        if "output_dbs" in self.params:
            for o in self.params["output_dbs"]:
                o['db_table'] = gdtc.aux.db.create_random_table_name()
        else:
            self.params["output_dbs"] = []
            for i in range(num):
                self.params["output_dbs"].append(self.__create_random_output())
        return self.params["output_dbs"]

    def __create_random_output(self):
        base_output = {**self.get_inputs()[0]} # Shallow copy of input params
        base_output['db_table'] = gdtc.aux.db.create_random_table_name()
        return base_output

# TODO: Think if the case of one or more tables/views in a BD to one or more tables/views in the same DB, should
# receive a special treatment

class DBs2FilesFilter(Filter):
    """
    Abstract base class for filters that take 1 or more input dbs and write something to 1 or more files.

    Params:
    input_paths : list of dbs (dictionaries as produced by gdtc.aux.db.create_db_and_table_dict()
    output_paths: list of paths (as strings)
    """
    def __init__(self, params={}):
        super(DBs2FilesFilter, self).__init__(params)

    def set_inputs(self, inputs):
        if inputs is not None:
            if isinstance(inputs, dict):
                self.params['input_dbs'] = [{**inputs}]  # Shallow copy
            else:  # If input_ is not iterable, a TypeError will be raised
                self.params['input_dbs'] = []
                for i in inputs:
                    self.params['input_dbs'].append({**i})  # Shallow copy
        else:
            self.params['input_dbs'] = []

    def set_outputs(self, outputs):
        if outputs is not None:
            if isinstance(outputs, str):
                self.params['output_paths'] = [outputs]
            else:  # This way if output is not iterable, a TypeError will be raised
                self.params['output_paths'] = []
                for o in outputs:
                    self.params['output_paths'].append(o)
        else:
            self.params['output_paths'] = []

    def get_inputs(self):
        return self.params['input_dbs']

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