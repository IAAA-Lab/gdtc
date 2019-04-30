FROM python:3.7-stretch

# Update base container install
RUN apt-get update
RUN apt-get upgrade -y

# Install Conda
RUN wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh;
RUN bash miniconda.sh -b -p /root/miniconda
ENV PATH /root/miniconda/bin:$PATH
RUN hash -r
RUN conda config --set always_yes yes --set changeps1 no
RUN conda update -q conda

# Useful for debugging any issues with conda
RUN conda info -a
COPY ./gdtc_conda.yml /root
RUN conda env create -f /root/gdtc_conda.yml
RUN conda init bash
ENV PATH /root/miniconda/envs/gdtc:$PATH
RUN echo "source activate gdtc" >> ~/.bashrc
RUN . activate gdtc

# Pip requirements must be installed in gdtc env, which have to be activated first,
# but sh has not source internal command
RUN /bin/bash -c "source activate gdtc && pip install minio && pip install luigi" 

# This will install gdal binaries such as ogr2ogr, shp2pgsql etc
RUN apt-get install -y gdal-bin

# This will install pgsql client
RUN apt-get install -y postgresql-client

RUN echo "GDTC_BASE_PROJECT"
RUN mkdir /gdtc
COPY . /gdtc

RUN apt-get -y install postgis
