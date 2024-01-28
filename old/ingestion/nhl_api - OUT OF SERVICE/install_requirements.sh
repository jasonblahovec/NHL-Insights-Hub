#!/bin/bash

apt update
apt install -y python3-pip
pip3 install requests 
pip3 install pandas 
pip3 install google-cloud-storage 
pip3 install pyspark 
pip3 install google-auth
pip3 install google-cloud-secret-manager
pip3 install jupyter

