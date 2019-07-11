from gdtc.filters.basefilters import FilterChain

def create_filter_chain(params, fs, first_input, last_output=None):
    """
    Add Filter f to FilterChain fc by taking the output_params of the last Filter in the FilterChain and
    making it the input_params of the Filter f.
    :param fc:
    :param f:
    :return:
    """

    fs[0].set_inputs(first_input)

    if last_output is not None:
        fs[-1].set_outputs(last_output)

    for i in range(1, len(fs)):
        prior_outputs = fs[i - 1].get_outputs()
        if prior_outputs: # if not None and not empty
            fs[i].set_inputs(prior_outputs)
        else:
            fs[i].set_inputs(fs[i-1].generate_random_outputs())
    return FilterChain(fs, params)

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

