from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from myUtils import readFiles,getSomeFileNames

if __name__ == "__main__":
    sc = SparkContext()
    fileNames = getSomeFileNames({2016:range(1,7)})
    print(fileNames)
    (taxi_data,prefix) = readFiles(fileNames,sc )
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
    tabSeparated.saveAsTextFile(sys.argv[0].split('.')[0] + "_valid.out")
    
    sc.stop()
    

