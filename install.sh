#!/bin/bash

sudo apt-get update
sudo apt-get -y install openmpi-common python3-distutils libopenmpi-dev python3-dev python3-pip

pip3 install scipy network numpy

wget https://bitbucket.org/mpi4py/mpi4py/downloads/mpi4py-3.0.1.tar.gz
tar -zxf mpi4py-3.0.1.tar.gz
cd mpi4py-3.0.1
python3 setup.py build
python3 setup.py install
mpiexec -n 5 python3 -m mpi4py.bench helloworld
