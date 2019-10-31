#!/bin/bash

sudo apt-get install python3-pip
pip3 install virtualenv
virtualenv bdp-ass2
source bdp-ass2/bin/activate
pip install -r requirements.txt
