import aux.file
from filters.basefilters import FilterChain


def create_file_filter_chain(params, fs, first_input_path=None, last_output_path=None):
    fs[0].set_input_path(first_input_path)
    # The output of the first Filter in the chain will be a temporary file
    fs[0].set_output_path(aux.file.create_tmp_file())

    # If last_output_path is None, it means we can use any temporary file to store the final result
    if last_output_path is None:
        fs[-1].set_output_path(aux.file.create_tmp_file())
    else:
        fs[-1].set_output_path(last_output_path)

    for i in range(1, len(fs)):
        fs[i].set_input_path(fs[i - 1].get_output_path())

    return FilterChain(params, fs)

# TODO: this works as long as the filter chain ends in a file and the filter starts with one. We will need
# similar solutions for other kinds of filters and chains
def append_filter_to_chain(fc, f):
    """
    Add Filter f to FilterChain fc by taking the output_path of the last Filter in the FilterChain and
    making it the input path of the Filter f.
    :param fc:
    :param f:
    :return:
    """
    f.set_input_path(fc.get_filters()[-1].get_output_path())
    fc.append_filter(f)
    return fc