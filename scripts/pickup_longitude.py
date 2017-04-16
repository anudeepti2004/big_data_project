from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader
from myUtils import readFiles,getSomeFileNames

if __name__ == "__main__":
    sc = SparkContext()
    fileNames = getSomeFileNames({2016:range(1,7)})
    (taxi_data,prefix) = readFiles(fileNames,sc )
    def checkValid(f,range):
        try:
            ff = float(f)
            if ff > 180 or ff < -180:
                return "Invalid"
            elif  range[1] >ff> range[0]:
                return "Valid"
            else:
                return "Invalid_NotNYC"
        except ValueError:
            return "Invalid_NotFloat"

    lon = (-74.259090,-73.700272)    
    field = taxi_data.map(lambda entry: (checkValid(entry[5],lon),1)).reduceByKey(lambda x,y: x+y)
    
    tabSeparated =  field.map(lambda x: x[0]+"\t"+str(x[1])) 
    tabSeparated.saveAsTextFile("pickup_lon.out")
    
    sc.stop()
