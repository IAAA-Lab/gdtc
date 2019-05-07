[![Build Status](https://travis-ci.org/IAAA-Lab/gdtc.svg?branch=master)](https://travis-ci.org/IAAA-Lab/gdtc)

# IAAA GeoData ToolChain (GDTC)
This is a library to create ETL (Extraction, Transformation and Load) pipelines in Python, with a strong emphasis on geographic data transformation.
GDTC will leverage existing geographic data processing tools (PostGIS, GDAL, etc.) and will provide a uniform access to their capabilities.
GDTC does not intend neither to provide a GUI to develop the ETL pipelines nor to develop its own language to express them. We believe that Python is the right tool for this
purpose. We just intend to make useful tools readily available to the developers of geographic ETL scripts in that language.

## Installation

You need Python and the Conda environment manager.

There is a Conda environment file named `gdtc_conda.yml`. You can create the environment and download the required modules running `conda env create -f gdtc_conda.yml`. If you prefer, 
you can use Anaconda Navigator (Environments > Import) with that file. In any case you will have a conda environment named `gdtc` in your computer.

After that, you can run `conda activate gdtc` to activate the environment, or use Anaconda Navigator instead.

## Docker

You can use Docker to install the whole application with all it's dependecies, including the Postgis working database.

In order to do that, follow those steps:

Clone this repository and go to that directory:
```
git clone https://github.com/IAAA-Lab/gdtc.git
cd gdtc
```

Then, you need to define a number of environment variables. You can create a 
shell file (e.g. set_env.sh) with these contents:

```
#!/bin/sh
export LOCAL_IN_VOL=${HOME}/input_files
export LOCAL_OUT_VOL=${HOME}/output_files
export POSTGIS_HOST=postgis
export POSTGIS_PORT=5432
export POSTGIS_USER=postgres
export POSTGIS_DATABASE=postgres
export POSTGIS_PASS=geodatatoolchainps
export GDTC_IN_VOL=/input
export GDTC_OUT_VOL=/output
export GDTC_ACCESS_KEY=YOUR_S3_ACCESS_KEY
export GDTC_SECRET_KEY=YOUR_S3_SECRET_KEY
```

and then to download the test data, build and run docker images and run the tests:

```
source set_env.sh
make all
```

This way you will run the tests on a fully configurated instance of a GDTC container with some sample data.

## License
This work is subject to the European Union Public License 1.2 which can be read in the LICENSE file. Regarding the attribution obligation in that license, this work is:

Copyright 2019 by the [IAAA (Advanced Information Systems Laboratory)](https://www.iaaa.es).

Contributors:

- [Rubén Béjar](https://www.rubenbejar.com)
- [Víctor Fernández Melic](https://github.com/Melic93)
