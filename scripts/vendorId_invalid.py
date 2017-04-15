from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from myUtils import readFiles

if __name__ == "__main__":
	sc = SparkContext()
	(taxi_data,prefix) = readFiles(['data/yellow_tripdata_2016-01.csv','data/yellow_tripdata_2016-02.csv','data/yellow_tripdata_2016-03.csv','data/yellow_tripdata_2016-04.csv','data/yellow_tripdata_2016-05.csv','data/yellow_tripdata_2016-06.csv'],sc )
	def checkValid(vendor_id):
		try:
			int(vendor_id)
			if int(vendor_id) == 1 or int(vendor_id) == 2:
				return "Valid"
			else:
				return "Invalid"
		except ValueError:
			return "Invalid"
		
	vendorID = taxi_data.map(lambda entry: (checkValid(entry[0]),1)).reduceByKey(lambda x,y: x+y)
	
	tabSeparated =  vendorID.map(lambda x: x[0]+"\t"+str(x[1])) 
    	tabSeparated.saveAsTextFile("vendorId_valid.out")
	
	sc.stop()
	

