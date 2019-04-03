FROM python:3.7-stretch

# Update base container install
RUN apt-get update
RUN apt-get upgrade -y

# Add unstable repo to allow us to access latest GDAL buildsdocker i
RUN echo deb http://ftp.uk.debian.org/debian unstable main contrib non-free >> /etc/apt/sources.list
RUN apt-get update

# Existing binutils causes a dependency conflict, correct version will be installed when GDAL gets intalled
RUN apt-get remove -y binutils

# Install GDAL dependencies
RUN apt-get -t unstable install -y libgdal-dev g++

# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# This will install latest version of GDAL and luigi framework
RUN pip install GDAL>=2.2.4
RUN pip install luigi

# This will install gdal binaries such as ogr2ogr, shp2pgsql etc
RUN apt-get install -y gdal-bin

# This will install pgsql client
RUN apt-get install -y postgresql-client

# Clone GDTC git repository
RUN git clone https://github.com/IAAA-Lab/gdtc.git