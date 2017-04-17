from __future__ import print_function

import sys
from csv import reader
from operator import add
from validation_utils import *
from myUtils import *

'''
{0: 'VendorID', 'VendorID': 0, 2: 'tpep_dropoff_datetime', 3: 'passenger_count', 4: 'trip_distance', 5: 'pickup_longitude', 6: 'pickup_latitude', 'extra': 13, 8: 'store_and_fwd_flag', 9: 'dropoff_longitude', 10: 'dropoff_latitude', 11: 'payment_type', 12: 'fare_amount', 13: 'extra', 14: 'mta_tax', 15: 'tip_amount', 16: 'tolls_amount', 17: 'improvement_surcharge', 18: 'total_amount', 'RatecodeID': 7, 'trip_distance': 4, 1: 'tpep_pickup_datetime', 7: 'RatecodeID', 'fare_amount': 12, 'pickup_longitude': 5, 'dropoff_latitude': 10, 'tolls_amount': 16, 'tip_amount': 15, 'mta_tax': 14, 'dropoff_longitude': 9, 'tpep_pickup_datetime': 1, 'tpep_dropoff_datetime': 2, 'improvement_surcharge': 17, 'total_amount': 18, 'passenger_count': 3, 'payment_type': 11, 'store_and_fwd_flag': 8, 'pickup_latitude': 6}
'''

sc = SparkContext()
fileNames = getAllFileNames()
fields = getFieldDic()
(taxi_data,prefix) = readFiles(fileNames,sc )

# VendorID
vendorID = taxi_data.map(lambda entry: (checkVendorIDValid(entry[fields['VendorID']]),1)).reduceByKey(lambda x,y: x+y)
vendor_out = vendorID.map(lambda x: x[0]+"\t"+str(x[1]))
vendor_out.saveAsTextFile("vendorID_valid.out")

# Drop-off Time
dropoff_time = taxi_data.map(lambda entry: (checkPickUpDateValid(entry[fields['tpep_dropiff_datetime']]),1)).reduceByKey(lambda x,y: x+y)
dropoff_time_out +=  dropiff_time.map(lambda x: x[0]+"\t"+str(x[1]))
dropoff_time.saveAsTextFile(sys.argv[0].split('.')[0] + "_valid.out")

# Pick-up Time
pickup_time = taxi_data.map(lambda entry: (checkDropoffDateValid(entry[fields['tpep_pickup_datetime']]),1)).reduceByKey(lambda x,y: x+y)
pickup_time_out +=  pickup_time.map(lambda x: x[0]+"\t"+str(x[1]))
pickup_time.saveAsTextFile(sys.argv[0].split('.')[0] + "_valid.out")

# Passenger Count
passenger_count = taxi_data.map(lambda entry: (checkPassengerCountValid(entry[fields['passenger_count']]),1)).reduceByKey(lambda x,y: x+y)
passcount_out = passenger_count.map(lambda x: x[0]+"\t"+str(x[1]))
passcount_out.saveAsTextFile("vendorID_valid.out")

#Trip Distance
trip_distance = taxi_data.map(lambda entry: (checkTripDistanceValid(entry[fields['trip_distance']]),1)).reduceByKey(lambda x,y: x+y)
distance_out = trip_distance.map(lambda x: x[0]+"\t"+str(x[1]))
distance_out.saveAsTextFile("trip_distance_valid.out")

# Rate Code
rate_code = taxi_data.map(lambda entry: (checkRateCodeIdValid(entry[fields['RatecodeID']]),1)).reduceByKey(lambda x,y: x+y)
ratecode_out = rate_code.map(lambda x: x[0]+"\t"+str(x[1]))
ratecode_out.saveAsTextFile("RatecodeID_valid.out")

