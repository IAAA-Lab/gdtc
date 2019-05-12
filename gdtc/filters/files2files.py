import logging
import subprocess

from gdtc.filters.basefilters import Files2FilesFilter


class MosaicRasters(Files2FilesFilter):
    """
    Takes n input rasters and produces 1 output raster mosaicing the input.
    """
    def run(self):
        output = self.get_output()[0]
        logging.debug(f' Executing MosaicRasters Filter with params: {self.params}')
        cmd = f'gdal_merge.py -o {output}'
        for input_path in self.get_input():
            cmd = cmd + f'  {input_path}'

        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            msg = f' Error executing gdal_merge.py command: {e}'
            logging.error(msg)
            raise RuntimeError(msg) # If we fail, run must end with an error

        logging.debug(f' MosaicRasters output: {output}')

