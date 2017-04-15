from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from myUtils import readFiles

if __name__ == "__main__":
	sc = SparkContext()
	(taxi_data,prefix) = readFiles(['data/yellow_tripdata_2015-*.csv','data/yellow_tripdata_2016-01.csv','data/yellow_tripdata_2016-02.csv','data/yellow_tripdata_2016-03.csv','data/yellow_tripdata_2016-04.csv','data/yellow_tripdata_2016-05.csv','data/yellow_tripdata_2016-06.csv'],sc )
	def checkValid(tolls_amount):
		try:
			num = float(tolls_amount)
			if  num > 0:
				return "Valid"
			elif num == 0:
				return "Valid_ZeroToll"
			else:
				return "Invalid_NegativeToll"
		except ValueError:
			return "Invalid_NotFloat"
		
	field = taxi_data.map(lambda entry: (checkValid(entry[17]),1)).reduceByKey(lambda x,y: x+y)
	
	tabSeparated =  field.map(lambda x: x[0]+"\t"+str(x[1])) 
    	tabSeparated.saveAsTextFile("tolls.out")
	
	sc.stop()
	

