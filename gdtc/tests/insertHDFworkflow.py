import luigi
import gdtc.filters.file2file as f2f
import gdtc.tasks.workflowbuilder as wfb


# TODO: probar con un fichero HDF de verdad, que ahora falla por eso
if __name__ == '__main__':
    f1 = f2f.hdf2tif(layer_num=0, dstSRS="EPSG:4358")
    _,coord_sys = str(f1.getParams()['dstSRS']).split(':')
    f2 = f2f.tif2sql(coord_sys = coord_sys, table = "table_dummy", db = "db_dummy")
    filterchain = f2f.FileFilterChain([f1,f2], "fichero_entrada", "fichero_salida", params={})
    filterchain.run()


    workflow = wfb.filter_chain_2_task_chain(filterchain)
    #t1 = wfb.createFileTransformationTask(f1.getInputPath(), f1.getOutputPath(), f1.run)
    #t2 = wfb.createFileTransformationTask(f1.getOutputPath(), "salida.sql", f2.run)
    #workflow = wfb.createSequence(ts=[t1, t2])
    luigi.build([workflow])