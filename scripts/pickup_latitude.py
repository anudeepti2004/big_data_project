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
	        if ff > 90 or ff < -90:
	        	return "Invalid"
	        elif  range[1] >ff> range[0]:
	            return "Valid"
	        else:
	            return "Outlier_NotNYC"
	    except ValueError:
	        return "Invalid_NotFloat"

	lat = (40.477399,40.917577)  
    field = taxi_data.map(lambda entry: (checkValid(entry[6],lat),1)).reduceByKey(lambda x,y: x+y)
    
    tabSeparated =  field.map(lambda x: x[0]+"\t"+str(x[1])) 
    tabSeparated.saveAsTextFile("pickup_lat.out")
    
    sc.stop()
