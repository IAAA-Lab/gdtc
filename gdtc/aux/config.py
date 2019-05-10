import sys
import os
import logging

class Config():
    GDTC_IN_VOL = os.getenv('GDTC_IN_VOL') or '/input'
    GDTC_OUT_VOL = os.getenv('GDTC_OUT_VOL') or '/output'
    POSTGIS_HOST=os.getenv('POSTGIS_HOST') or 'postgis'
    POSTGIS_PORT=os.getenv('POSTGIS_PORT') or 5432
    POSTGIS_USER=os.getenv('POSTGIS_USER') or 'postgres'
    POSTGIS_PASS=os.getenv('POSTGIS_PASS') or 'geodatatoolchainps'
    POSTGIS_DATABASE=os.getenv('POSTGIS_DATABASE') or 'postgres'
    GDTC_LOG_FILE=os.getenv('GDTC_LOG_FILE') or 'app.log'
    GDTC_ACCESS_KEY=os.getenv('GDTC_ACCESS_KEY')
    GDTC_SECRET_KEY= os.getenv('GDTC_SECRET_KEY')
    
    logging.basicConfig(filename=f'{GDTC_OUT_VOL}/{GDTC_LOG_FILE}', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)

    gdtc_in_vol_log = os.getenv('GDTC_IN_VOL') if os.getenv('GDTC_IN_VOL') is not None else '[Not defined, using default value \'/input\']'
    gdtc_out_vol_log = os.getenv('GDTC_OUT_VOL') if os.getenv('GDTC_OUT_VOL') is not None else '[Not defined, using default value \'/output\']'
    postgis_host_log = os.getenv('POSTGIS_HOST') if os.getenv('POSTGIS_HOST') is not None else '[Not defined, using default value \'postgis\']'
    postgis_port_log = os.getenv('POSTGIS_PORT') if os.getenv('POSTGIS_PORT') is not None else '[Not defined, using default value 5432]'
    postgis_user_log = os.getenv('POSTGIS_USER') if os.getenv('POSTGIS_USER') is not None else '[Not defined, using default value \'postgres\']'
    postgis_pass_log = '[Secret Value]' if os.getenv('POSTGIS_PASS') is not None else '[Not defined, using default value \'geodatatoolchainps\']'
    postgis_database_log = os.getenv('POSTGIS_DATABASE') if os.getenv('POSTGIS_DATABASE') is not None else '[Not defined, using default value \'postgres\']'
    gdtc_log_file_log = os.getenv('GDTC_LOG_FILE') if os.getenv('GDTC_LOG_FILE') is not None else '[Not defined, using default value \'app.log\']'
    gdtc_access_key_log = '[Secret Value]' if os.getenv('GDTC_ACCESS_KEY') is not None else '[Not defined]'
    gdtc_secret_key_log = '[Secret Value]' if os.getenv('GDTC_SECRET_KEY') is not None else '[Not defined]'

    logging.debug(
        f'\nEnvironment variables defined:,\n\
        GDTC_IN_VOL={gdtc_in_vol_log},\n\
        GDTC_OUT_VOL={gdtc_out_vol_log},\n\
        POSTGIS_HOST={postgis_host_log},\n\
        POSTGIS_PORT={postgis_port_log},\n\
        POSTGIS_USER={postgis_user_log},\n\
        POSTGIS_PASS={postgis_pass_log},\n\
        POSTGIS_DATABASE={postgis_database_log},\n\
        GDTC_LOG_FILE={gdtc_log_file_log},\n\
        GDTC_ACCESS_KEY={gdtc_access_key_log},\n\
        GDTC_SECRET_KEY={gdtc_secret_key_log}\
    ')    
