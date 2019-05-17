from gdtc.filters.basefilters import FilterChain

# TODO: We are not checking that the filters can really be connected. May be it's a user responsability
#       to check that any connection made is of the type DB_out -> BD_in or File_out -> File_in

def create_filter_chain(params, fs, first_input, last_output=[]):
    """
    Add Filter f to FilterChain fc by taking the output_params of the last Filter in the FilterChain and
    making it the input_params of the Filter f.
    :param fc:
    :param f:
    :return:
    """

    fs[0].set_inputs(first_input)
    fs[-1].set_outputs(last_output)

    for i in range(1, len(fs)):
        try:
            fs[i].set_inputs(fs[i - 1].get_outputs())
        except KeyError as e:
            # If there are not output_paths in fs[i-1] generate them
            fs[i].set_inputs(fs[i-1].generate_random_outputs(1))
    
    return FilterChain(params, fs)

def append_filter_to_chain(fc, f):
    """
    Add Filter f to FilterChain fc by taking the output of the last Filter in the FilterChain and
    making it the input of the Filter f.
    :param fc:
    :param f:
    :return:
    """

    try:
        f.set_inputs(fc.get_filters()[-1].get_outputs())
    except KeyError as e:
        # If there are not output_paths in fc.get_filters()[-1] generate them
        f.set_inputs(fc.get_filters()[-1].generate_random_outputs(1))

    fc.append_filter(f)

    return fc

