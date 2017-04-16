from __future__ import print_function

import sys
from operator import add
from csv import reader

def readFiles (files,sc):
    concatenatedFiles = ','.join(files)
    
    csvfile = sc.textFile(concatenatedFiles)
    header = csvfile.first()

    csvfile = csvfile.filter(lambda line : line != header)
    # taxi_data.map(lambda t: map(float,t[5:7]))
    
    taxi_data = csvfile.mapPartitions(lambda x: reader(x))
    if "yellow" in concatenatedFiles:
        return (taxi_data,"yellow")
    else:
        return (taxi_data,"green")
    
OUR_DATABASE_PATH = '/user/dv697/data/yellow_tripdata_'
def readAllFiles (sc):
    cy_m_dic = dict((y, [k for k in range(1,13)]) for y in range(2013,2017))
    return readFiles2(y_m_dic,sc)

OUR_DATABASE_PATH = '/user/dv697/data/yellow_tripdata_'

def readFiles2 (year_months_dic,sc):
    basePath = OUR_DATABASE_PATH
    new_type = []
    old_type = []
    for y,m_array in year_months_dic.items():
        for m in m_array:
            # No gps 
            if y == 2016 and m > 6:
                new_type.append(basePath + '%d-%02d.csv' %(y,m))
            else:
                old_type.append(basePath + '%d-%02d.csv' %(y,m))

    oldTypeFiles = ','.join(old_type)
    newTypeFiles = ','.join(new_type)


    csvfile = sc.textFile(oldTypeFiles)
    header = csvfile.first()

    csvfile = csvfile.filter(lambda line : line != header)
    # taxi_data.map(lambda t: map(float,t[5:7]))
    
    taxi_data = csvfile.mapPartitions(lambda x: reader(x))

    csvfile2 = sc.textFile(newTypeFiles)
    header2 = csvfile2.first()

    csvfile2 = csvfile2.filter(lambda line : line != header2)
    # taxi_data.map(lambda t: map(float,t[5:7]))
    
    ## Assign GPS coordinates to each place
    taxi_data2 = csvfile2.mapPartitions(lambda x: reader(x)).map(lambda a: a[:5] + [0.0, 0.0] + a[5:7] + [0.0, 0.0] + a[9:])

    taxi_data.union(taxi_data2)

    if "yellow" in oldTypeFiles + newTypeFiles:
        return (taxi_data,"yellow")
    else:
        return (taxi_data,"green")

def getAllFileNames():
    y_m_dic = dict((y, [k for k in range(1,13)]) for y in range(2013,2017))
    return getSomeFileNames(y_m_dic)

def getSomeFileNames(year_months_dic):
    basePath = OUR_DATABASE_PATH
    file_names = []
    for y,m_array in year_months_dic.items():
        for m in m_array:
            file_names.append(basePath + '%d-%02d.csv' %(y,m))
    return file_names

class coordinateMapper(object):
    def __init__(self,path='zones.pickle'):
        import pickle
        import matplotlib.path as mplPath
        import numpy as np
        self.all_poly=pickle.load(open(path,'rb'))
        print(self.all_poly[15])
        
    def convert(self,xy):
        #xy is a tuple(x,y)
        for i,poly in self.all_poly:
            if poly.contains_point(xy):
                print('yes')
                return i
        
def getConverterFunc():
    mapper = coordinateMapper()
    return lambda t: mapper.convert(t)

def checkValid(f,range):
	    try:
	        ff = float(f)
	        if  range[1] >f > range[0]:
	            return "Valid"
	        else:
	            return "Invalid_NotNYC"
	    except ValueError:
	        return "Invalid_NotFloat"
