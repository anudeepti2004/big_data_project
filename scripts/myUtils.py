from __future__ import print_function

import sys
from operator import add
from csv import reader
import pickle


# d = getFieldDic() and then you can call d[4] and it returns 'trip_distance' OR the other way around d['trip_distance']=4.
_fields = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'RatecodeID', 'store_and_fwd_flag', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']
_fieldsDic = dict((_fields[i],i) for i in range(len(_fields)))
_fieldsDic.update(dict((i,_fields[i]) for i in range(len(_fields))))

def getFieldDic():
    return _fieldsDic

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
    y_m_dic = dict((y, [k for k in range(1,13)]) for y in range(2013,2017))
    return readFiles2(y_m_dic,sc)

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

    if oldTypeFiles:
        csvfile = sc.textFile(oldTypeFiles)
        header = csvfile.first()

        csvfile = csvfile.filter(lambda line : line != header)
        # taxi_data.map(lambda t: map(float,t[5:7]))
    
        taxi_data = csvfile.mapPartitions(lambda x: reader(x))
    else:
        csvfile = sc.textFile(newTypeFiles)
        header = csvfile.first()

        csvfile = csvfile.filter(lambda line : line != header)
        # taxi_data.map(lambda t: map(float,t[5:7]))
    
        taxi_data = csvfile.mapPartitions(lambda x: reader(x))
        zones_mean = pickle.load(open('scripts/zones_mean.pickle','r'))
        taxi_data = csvfile.mapPartitions(lambda x: reader(x)).map(lambda a: a[:5] + zones_mean[int(a[7])] + a[5:7] + zones_mean[int(a[8])] + a[9:])

    if oldTypeFiles and newTypeFiles:
        csvfile2 = sc.textFile(newTypeFiles)
        header2 = csvfile2.first()

        csvfile2 = csvfile2.filter(lambda line : line != header2)
        # taxi_data.map(lambda t: map(float,t[5:7]))
        
        ## Assign GPS coordinates to each place
        zones_mean = pickle.load(open('scripts/zones_mean.pickle','r'))
        taxi_data2 = csvfile2.mapPartitions(lambda x: reader(x)).map(lambda a: a[:5] + zones_mean[int(a[7])] + a[5:7] + zones_mean[int(a[8])] + a[9:])
        taxi_data.union(taxi_data2)

    ## Convert VendorID
    def convertVendorInt(x):
        i = _fieldsDic['VendorID']
        if x[i] == 'CMT': x[i]=1
        elif x[i] == 'VTS': x[i]=0
        return x

    taxi_data.map(convertVendorInt).filter(lambda x: len(x)!=0) # There are 1 empty array for each file. So lets remove them.

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
