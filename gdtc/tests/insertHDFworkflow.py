import luigi
import gdtc.filters.file2file as f2f
import gdtc.tasks.workflowbuilder as wfb


# TODO: probar con un fichero HDF de verdad, que no tenía uno a mano y peta
if __name__ == '__main__':
    # We create 2 filters
    f1 = f2f.hdf2tif(layer_num=0, dstSRS="EPSG:4358")
    _,coord_sys = str(f1.getParams()['dstSRS']).split(':')
    f2 = f2f.tif2sql(coord_sys = coord_sys, table = "table_dummy", db = "db_dummy")

    # We chain them
    filterchain = f2f.FileFilterChain([f1,f2], "fichero_entrada", "fichero_salida", params={})
    # We must simply run them in order
    filterchain.run()

    # We may create and run a luigi workflow instead
    workflow = wfb.filter_chain_2_task_chain(filterchain)
    luigi.build([workflow])