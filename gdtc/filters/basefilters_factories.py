from gdtc.filters.basefilters import FilterChain

# TODO: We are not checking that the filters can really be connected. May be it's a user responsability
#       to check that any connection made is of the type DB_out -> BD_in or File_out -> File_in

def create_filter_chain(params, fs, first_input, last_output=None):
    """
    Add Filter f to FilterChain fc by taking the output_params of the last Filter in the FilterChain and
    making it the input_params of the Filter f.
    :param fc:
    :param f:
    :return:
    """

    fs[0].set_input(first_input)
    fs[-1].set_output(last_output)

    for i in range(1, len(fs)):
        fs[i].set_input(fs[i - 1].get_output())
    
    return FilterChain(params, fs)

def append_filter_to_chain(fc, f):
    """
    Add Filter f to FilterChain fc by taking the output of the last Filter in the FilterChain and
    making it the input of the Filter f.
    :param fc:
    :param f:
    :return:
    """

    f.set_input(fc.get_filters()[-1].get_output())
    fc.append_filter(f)

    return fc

