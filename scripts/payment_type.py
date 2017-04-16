from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from myUtils import readFiles

if __name__ == "__main__":
	sc = SparkContext()
	(taxi_data,prefix) = readFiles(['data/yellow_tripdata_2016-01.csv','data/yellow_tripdata_2016-02.csv','data/yellow_tripdata_2016-03.csv','data/yellow_tripdata_2016-04.csv','data/yellow_tripdata_2016-05.csv','data/yellow_tripdata_2016-06.csv'],sc )
	def checkValid(payment_type):
		try:
			int(payment_type)
			if int(payment_type) > 0 and int(payment_type) <= 6 :
				return "Valid"
			else:
				return "Invalid_NotWithinRange"
		except ValueError:
			return "Invalid_NotInteger"
		
	field = taxi_data.map(lambda entry: (checkValid(entry[11]),1)).reduceByKey(lambda x,y: x+y)
	
	tabSeparated =  field.map(lambda x: x[0]+"\t"+str(x[1])) 
    	tabSeparated.saveAsTextFile(sys.argv[0].split('.')[0] + "_valid.out")
	
	sc.stop()
	

