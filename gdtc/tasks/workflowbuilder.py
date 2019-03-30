import luigi
import types
import gdtc.tasks.basetasks as basetasks

# Task builders and modifiers

def make_require(t1, t2):
    """
    Task t1 will require Task t2 (an only that).
    :param t1:
    :param t2:
    :return: t1
    """
    # It has to be a types.MethodType. You can't give a normal function to an object because that
    # will not be a method of that object (e.g. it won't have access to self)
    t1.requires = types.MethodType((lambda self : t2), t1)
    return t1

def make_require_many(t1, ts):
    """
    Task t1 will require all Tasks in ts (and only those)
    :param t1:
    :param ts:
    :return: t1
    """
    t1.requires = types.MethodType((lambda self: ts), t1)
    return t1


def create_task_chain(ts):
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
    t1 = basetasks.File2FileTask(inputPath = f.getInputPath(), outputPath = f.getOutputPath())
    t1.run = types.MethodType((lambda self : f.run()), t1)
    return t1

# Requiere una secuencia de filtros ya enlazados (input de uno es output del anterior)
def filter_chain_2_task_chain(filterChain):
    ts = []
    for f in filterChain.getFs():
        ts.append(create_file_2_file_task(f))
    return create_task_chain(ts)


