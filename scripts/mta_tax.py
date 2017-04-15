from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from myUtils import readFiles

if __name__ == "__main__":
	sc = SparkContext()
	(taxi_data,prefix) = readFiles(['data/yellow_tripdata_2016-01.csv','data/yellow_tripdata_2016-02.csv','data/yellow_tripdata_2016-03.csv','data/yellow_tripdata_2016-04.csv','data/yellow_tripdata_2016-05.csv','data/yellow_tripdata_2016-06.csv'],sc )
	def checkValid(mta_tax):
		try:
			num = float(mta_tax)
			if  num > 0:
				return "Valid"
			elif num == 0:
				return "Valid_ZeroTax"
			else:
				return "Invalid_NegativeMtaTax"
		except ValueError:
			return "Invalid_NotFloat"
		
	field = taxi_data.map(lambda entry: (checkValid(entry[14]),1)).reduceByKey(lambda x,y: x+y)
	
	tabSeparated =  field.map(lambda x: x[0]+"\t"+str(x[1])) 
    	tabSeparated.saveAsTextFile("mta_info.out")
	
	sc.stop()
	

