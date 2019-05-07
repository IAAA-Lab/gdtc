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

`git clone https://github.com/IAAA-Lab/gdtc.git` <br />
`cd gdtc/deployment` <br />
`docker build -t gdtc/base:latest ..` <br />
`docker-compose up -d` <br />
`docker exec -ti gdtc /bin/bash` <br />

This way you will get connected to a fully configurated instance of a GDTC container.

In order to check connection with Postgis container, try next command:

`psql -h $POSTGIS_HOST -p $POSTGIS_PORT -d postgres -U postgres`

If everything is ok, you should see: _postgres=#_

## License
This work is subject to the European Union Public License 1.2 which can be read in the LICENSE file. Regarding the attribution obligation in that license, this work is:

Copyright 2019 by the [IAAA (Advanced Information Systems Laboratory)](https://www.iaaa.es).

Contributors:

- [Rubén Béjar](https://www.rubenbejar.com)
- [Víctor Fernández Melic](https://github.com/Melic93)
