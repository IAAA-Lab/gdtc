import luigi
import types
import gdtc.tasks.basetasks as basetasks

# Task builders and modifiers

def make_require(t1, t2):
    """
    Task t1 will require Task t2 (an only that) or all the Tasks in sequence ts (and only those).
    :param t1:
    :param t2: a single Task or a sequence of Tasks
    :return: t1
    """
    # It has to be a types.MethodType. You can't give a normal function to an object because that
    # will not be a method of that object (e.g. it won't have access to self)
    t1.requires = types.MethodType((lambda self : t2), t1)
    return t1

def create_task_chain(ts):
    """
    Takes a sequence of tasks and creates a luigi workflow by making the i-nth task to require the (i-1)-nth
    :param ts:
    :return: the last one of the sequence. Running that one with luigi will run the whole chain because they
    are all linked by their requires methods
    """
    for i in range(1, len(ts)):
        # The commented line would not work. I assume it is because the closure of the
        # lambda can't capture the i variable
        # ts[i].requires = types.MethodType((lambda self : ts[i-1]), ts[i])
        make_require(ts[i], ts[i - 1])
    return ts[-1]


def create_file_2_file_task(f):
    """

    :param f: a File2FileFilter
    :return: a File2FileTask
    """
    t1 = basetasks.File2FileTask(input_path = f.get_input_path(), output_path = f.get_output_path())
    t1.run = types.MethodType((lambda self : f.run()), t1)
    return t1

# TODO: Managing inputs and outputs requires some extra work, or we can assume that the File2FileTask does
# already that?
def create_file_2_file_task_subclass(f):
    """
    Creates a Task class, that subclasses File2FileTask and runs the File2FileFilter f.
    TODO: complete this explaining how it manages luigi Params
    :param f:
    :return:
    """
    # We will add the params in f to the properties of the class we create
    class_properties = {}
    params = f.get_params()
    for k, v in params.items():
        class_properties[k] = luigi.Parameter(v) # We can give a value to the parameter at the class level, but we
        # can still override it at the object level with luigi Parameters magic

    # We will also add a proper constructor that calls its super, parses the Luigi params and
    # makes them available to the f object, for instance for using them in run
    def newinit(self, *args, **kwargs):
        super(basetasks.File2FileTask, self).__init__(*args, **kwargs)
        fparams = {}
        # For every (param_name, Param object (that we don't need) in the Task
        for k, _ in self.get_params():
            # self.aparam would allow us to access to the value of the param named aparam.
            # This is the dynamic version, where that value is stored in fparams
            fparams[k] = getattr(self, k)
        # and finally given to the f object so they are available when needed
        f.set_params(fparams)
    class_properties['__init__'] = newinit

    # And the run method of f
    class_properties['run'] = lambda self: f.run()

    # TODO: create a unique class name for each invocation, or maybe accept a parameter with a name
    # instead of TempClassName
    return type("TempClassName", (basetasks.File2FileTask,), class_properties)



# Requiere una secuencia de filtros ya enlazados (input de uno es output del anterior)
def filter_chain_2_task_chain(filterChain):
    ts = []
    for f in filterChain.get_filters():
        ts.append(create_file_2_file_task(f))
    return create_task_chain(ts)


