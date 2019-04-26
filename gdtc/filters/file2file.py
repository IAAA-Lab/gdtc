import subprocess

from osgeo import gdal
import geopandas
import matplotlib.pyplot as plt

from gdtc.filters.basefilters import File2FileFilter


class HDF2TIF(File2FileFilter):
    def run(self):
        # Load file and get layer
        hdf = gdal.Open(self.get_input(), gdal.GA_ReadOnly)
        src_ds = gdal.Open(hdf.GetSubDatasets()[int(self.params['layer_num'])][0], gdal.GA_ReadOnly)

        # Ojo con los tipos, asumimos que reproject es bool etc.
        if self.params['reproject']:
            warp_options = gdal.WarpOptions(srcSRS=self.params["srcSRS"],
                                            dstSRS=self.params["dstSRS"],
                                            xRes=self.params["cell_res"], yRes=self.params["cell_res"],
                                            errorThreshold=0,
                                            resampleAlg=gdal.GRA_Average,
                                            warpOptions=['SAMPLE_GRID=YES', 'SAMPLE_STEP=1000', 'SOURCE_EXTRA=1000'])
            gdal.Warp(self.get_output(), src_ds, options=warp_options)

        else:
            # Generate file in tif format
            layer_array = src_ds.ReadAsArray()
            out = gdal.GetDriverByName('GTiff').Create(self.get_output(), src_ds.RasterXSize, src_ds.RasterYSize, 1,
                                                       gdal.GDT_Byte, ['COMPRESS=LZW', 'TILED=YES'])
            out.SetGeoTransform(src_ds.GetGeoTransform())
            out.SetProjection(src_ds.GetProjection())
            out.GetRasterBand(1).WriteArray(layer_array)
            # Write file to disk
            out = None
        return self.get_output()


class TIF2SQL(File2FileFilter):
    def run(self):
        # Generate sql file
        cmd = f'raster2pgsql -I -C -s {self.params["coord_sys"]} \"{self.get_input()}\" -F -d {self.params["table"]} > \"{self.get_output()}\"'
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        return self.get_output()

class PlotMap(File2FileFilter):
    def run(self):
        map = geopandas.read_file(self.get_input())
        map.plot()
        plt.savefig(self.get_output())
