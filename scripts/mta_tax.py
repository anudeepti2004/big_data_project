from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from myUtils import readFiles

if __name__ == "__main__":
	sc = SparkContext()
    fileNames = getSomeFileNames({2016:range(1,7)})
	(taxi_data,prefix) = readFiles(fileNames,sc )
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
	

