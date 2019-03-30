from osgeo import gdal
import subprocess

class File2FileFilter():
    """
    Base class for filters that take an input file and produce an output file.
    """
    def __init__(self, params):
        self.params = params

    # Ojo, igual habría que hacer una deepcopy defensiva, aunque en Python no sea muy habitual
    def getParams(self):
        return self.params

    def setInputPath(self, input_path):
        self.params['input_path'] = input_path

    def setOutputPath(self, output_path):
        self.params['output_path'] = output_path

    def getInputPath(self):
        return f'{self.params["input_path"]}.hdf'

    def getOutputPath(self):
        return f'{self.params["output_path"]}.tif'


class FileFilterChain(File2FileFilter):
    """
    A File2FileFilter which takes a sequence of File2Files as input, and runs them in order
    """
    def __init__(self, fs, first_input_path, last_output_path, params):
        super(self.__class__, self).__init__(params)
        fs[0].setInputPath(first_input_path)
        fs[0].setOutputPath(f'{first_input_path}_output')
        fs[-1].setOutputPath(last_output_path)

        for i in range(1, len(fs)):
            fs[i].setInputPath(fs[i-1].getOutputPath())
        self.fs = fs

    def getFs(self):
        return self.fs

    def run(self):
        for f in self.fs:
            f.run()


class HDF2TIF(File2FileFilter):
    def run(self):
        # Load file and get layer
        hdf = gdal.Open(self.getInputPath(), gdal.GA_ReadOnly)
        src_ds = gdal.Open(hdf.GetSubDatasets()[int(self.params['layer_num'])][0], gdal.GA_ReadOnly)

        # Ojo con los tipos, asumimos que reproject es bool etc.
        if self.params['reproject']:
            warp_options = gdal.WarpOptions(srcSRS=self.params["srcSRS"],
                                            dstSRS=self.params["dstSRS"],
                                            xRes=self.params["cell_res"], yRes=self.params["cell_res"],
                                            errorThreshold=0,
                                            resampleAlg=gdal.GRA_Average,
                                            warpOptions=['SAMPLE_GRID=YES', 'SAMPLE_STEP=1000', 'SOURCE_EXTRA=1000'])
            gdal.Warp(self.getOutputPath(), src_ds, options=warp_options)

        else:
            # Generate file in tif format
            layer_array = src_ds.ReadAsArray()
            out = gdal.GetDriverByName('GTiff').Create(self.getOutputPath(), src_ds.RasterXSize, src_ds.RasterYSize, 1,
                                                       gdal.GDT_Byte, ['COMPRESS=LZW', 'TILED=YES'])
            out.SetGeoTransform(src_ds.GetGeoTransform())
            out.SetProjection(src_ds.GetProjection())
            out.GetRasterBand(1).WriteArray(layer_array)
            # Write file to disk
            out = None
        return self.getOutputPath()


class TIF2SQL(File2FileFilter):
    def run(self):
        # Generate sql file
        cmd = f'raster2pgsql -I -C -s {self.params["coord_sys"]} \"{self.getInputPath()}\" -F -d {self.params["table"]} > \"{self.getOutputPath()}\"'
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        return self.getOutputPath()


# Factory methods to create filters
# TODO: Decidir qué hacemos para la gestión de parámetros, especialmente en filtros compuestos donde los
# distintos filtros individuales pueden interpretar de manera distinta algunos de ellos etc.
# TODO: Decidir si usamos el estilo diccionario de parámetros en general, o si tiene sentido tener este tipo
# de métodos factoría para facilitar al menos los usos más comunes
def hdf2tif(layer_num, input_file_name="NOT_ASSIGNED_YET", reproject=False, srcSRS=None, dstSRS=None, cell_res=None):
    params = {}
    params['input_path'] = f'{input_file_name}.hdf'
    params['output_path'] = f'{input_file_name}.tif'
    params['layer_num'] = layer_num
    params['reproject'] = reproject
    params['dstSRS'] = dstSRS
    params['cell_res'] = cell_res
    params['srcSRS'] = srcSRS

    return HDF2TIF(params)

def tif2sql(coord_sys, table, db, file_name = "NOT_ASSIGNED_YET"):
    params = {}
    params['input_path'] = f'{file_name}.tif'
    params['output_path'] = f'{file_name}.sql'
    params['coord_sys'] = coord_sys
    params['table'] = table
    params['db'] = db

    return TIF2SQL(params)