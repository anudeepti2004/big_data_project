from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def readFiles (files):
        concatenatedFiles = ','.join(files)

        csvfile = sc.textFile(concatenatedFiles)
        header = csvfile.first()

        csvfile = csvfile.filter(lambda line : line != header)

        taxi_data = csvfile.mapPartitions(lambda x: reader(x))
        if "yellow" in concatenatedFiles:
                return (taxi_data,"yellow")
        else:
                return (taxi_data,"green")
