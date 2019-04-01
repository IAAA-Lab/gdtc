# IAAA GeoData ToolChain (GDTC)
This library  will facilitate its users to create ETL (Extraction, Transformation and Load) pipelines in Python, with a strong emphasis on geographic data transformation.
GDTC will leverage existing geographic data processing tools (PostGIS, GDAL, etc.) and will provide a uniform access to their capabilities.
GDTC does not intend neithr to provide a GUI to develop the ETL pipelines nor to develop a language to express them. We believe that Python is perfect for this
purpose. We just intend to make useful tools easily available to develop geographic ETL scripts in that language.

## Installation

You need Python and the Conda environment manager.

There is a Conda environment file named `gdtc_conda.yml`. You can create the environment and download the required modules running `conda env create -f gdtc_conda.yml`. If you prefer, 
you can use Anaconda Navigator (Environments > Import) with that file. In any case you will have a conda environment named `gdtc` in your computer.

After that, you can run `conda activate gdtc` to activate the environment, or use Anaconda Navigator instead.
