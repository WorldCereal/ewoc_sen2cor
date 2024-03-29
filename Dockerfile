FROM ubuntu:20.04
LABEL maintainer="Fahd Benatia <fahd.benatia@csgroup.eu>"

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

## Install dataship and eotile
RUN apt-get update -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing --no-install-recommends \
        python3-pip \
        wget git \
        apt-utils file \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install -U pip

RUN pip3 install --no-cache-dir rfc5424-logging-handler

ARG EWOC_DATASHIP_VERSION=0.9.3
LABEL EWOC_DATASHIP="${EWOC_DATASHIP_VERSION}"
COPY ewoc_dag-${EWOC_DATASHIP_VERSION}.tar.gz /opt
RUN pip3 install /opt/ewoc_dag-${EWOC_DATASHIP_VERSION}.tar.gz

## Install sen2cor
RUN wget --quiet -P /opt http://step.esa.int/thirdparties/sen2cor/2.9.0/Sen2Cor-02.09.00-Linux64.run \
    && chmod +x /opt/Sen2Cor-02.09.00-Linux64.run \
    && ./opt/Sen2Cor-02.09.00-Linux64.run \
    && rm /opt/Sen2Cor-02.09.00-Linux64.run
# Copy custom L2A_GIPP.xml to sen2cor home
# This file can be copied to tmp and used as a param
COPY L2A_GIPP.xml /root/sen2cor/2.9/cfg/
RUN mkdir -p /root/sen2cor/2.9/dem/srtm

# Copy ESA CCI files (6Go)
#COPY ESACCI-LC-L4-ALL-FOR-SEN2COR.tar /tmp
# Extract the CCI files in the aux_data in the sen2cor_bin folder
#RUN tar -xvf /tmp/ESACCI-LC-L4-ALL-FOR-SEN2COR.tar -C /tmp/Sen2Cor-02.09.00-Linux64/lib/python2.7/site-packages/sen2cor/aux_data/

## Copy scripts
COPY . /
RUN pip install .
