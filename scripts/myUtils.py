from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def readFiles (files,sc):
	concatenatedFiles = ','.join(files)
	
    	csvfile = sc.textFile(concatenatedFiles)
	header = csvfile.first()

    	csvfile = csvfile.filter(lambda line : line != header)
	
	taxi_data = csvfile.mapPartitions(lambda x: reader(x))
	if "yellow" in concatenatedFiles:
		return (taxi_data,"yellow")
	else:
		return (taxi_data,"green")
    
OUR_DATABASE_PATH = '/user/dv697/data/yellow_tripdata_'
def getAllFileNames():
    y_m_dic = {y:[k for k in range(1,13)] for y in range(2013,2017)}
    return getSomeFileNames(y_m_dic)

def getSomeFileNames(year_months_dic):
    basePath = OUR_DATABASE_PATH
    file_names = []
    for y,m_array in year_months_dic.items():
        for m in m_array:
            file_names.append(basePath + '%d-%02d.csv' %(y,m))
    return file_names
