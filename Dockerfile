FROM ubuntu:18.04
LABEL maintainer="Fahd Benatia <fahd.benatia@csgroup.eu>"

WORKDIR /tmp

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
# Install dataship and eotile
RUN apt-get update -y && apt-get install -y python3-pip && apt-get install -y wget
RUN pip3 install -U pip
ADD eotile-0.2rc2-py3-none-any.whl /tmp
RUN pip3 install /tmp/eotile-0.2rc2-py3-none-any.whl

COPY dataship-0.1.2.tar.gz /tmp
RUN pip3 install /tmp/dataship-0.1.2.tar.gz

# Install sen2cor
RUN wget http://step.esa.int/thirdparties/sen2cor/2.9.0/Sen2Cor-02.09.00-Linux64.run
RUN chmod +x /tmp/Sen2Cor-02.09.00-Linux64.run
RUN ./Sen2Cor-02.09.00-Linux64.run
# Copy custom L2A_GIPP.xml to sen2cor home
# This file can be copied to tmp and used as a param
COPY L2A_GIPP.xml /root/sen2cor/2.9/cfg/
# Copy ESA CCI files (6Go)
#COPY ESACCI-LC-L4-ALL-FOR-SEN2COR.tar /tmp
# Extract the CCI files in the aux_data in the sen2cor_bin folder
#RUN tar -xvf /tmp/ESACCI-LC-L4-ALL-FOR-SEN2COR.tar -C /tmp/Sen2Cor-02.09.00-Linux64/lib/python2.7/site-packages/sen2cor/aux_data/
COPY run_s2c.py /tmp
ENTRYPOINT ["python3"]
