import tempfile
import datetime

# TODO: The file is crated with read/write permission for the user owner of the process that
# creates it. Make sure this will not cause trouble
def create_tmp_file():
    """
    Creates a temporary file that will not be deleted after closed (we do not know who will read it and when)
    :return: The name of this file
    """
    date_prefix = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    prefix = f'{date_prefix}_gdtctmp_'
    tmp = tempfile.NamedTemporaryFile(prefix = prefix, delete=False)
    tmp.close()
    return tmp.name

