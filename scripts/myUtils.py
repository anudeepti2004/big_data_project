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

    def convert(self,xy):
        #xy is a tuple(x,y)
        for i,poly in self.all_poly:
            if poly.contains_point(xy):
                return i
        
def getConverterFunc():
    mapper = coordinateMapper()
    return lambda t: mapper.convert(t)
