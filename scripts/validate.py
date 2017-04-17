from __future__ import print_function

import sys
from csv import reader
from operator import add
from validation_utils import *
from myUtils import *

sc = SparkContext()
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")

fields = getFieldDic()
(taxi_data,prefix) = readAllFiles(sc)

# VendorID
vendorID = taxi_data.map(lambda entry: (checkVendorIDValid(entry[fields['VendorID']]),1)).reduceByKey(lambda x,y: x+y)
vendor_out = vendorID.map(lambda x: x[0]+"\t"+str(x[1]))
vendor_out.saveAsTextFile("VendorID_valid.out")

# Drop-off Time
dropoff_time = taxi_data.map(lambda entry: (checkPickUpDateValid(entry[fields['tpep_dropiff_datetime']]),1)).reduceByKey(lambda x,y: x+y)
dropoff_time_out =  dropoff_time.map(lambda x: x[0]+"\t"+str(x[1]))
dropoff_time_outg.saveAsTextFile("tpep_dropiff_datetime_valid.out")

# Pick-up Time
pickup_time = taxi_data.map(lambda entry: (checkDropoffDateValid(entry[fields['tpep_pickup_datetime']]),1)).reduceByKey(lambda x,y: x+y)
pickup_time_out =  pickup_time.map(lambda x: x[0]+"\t"+str(x[1]))
pickup_time_out.saveAsTextFile("tpep_pickup_datetime_valid.out")

# Passenger Count
passenger_count = taxi_data.map(lambda entry: (checkPassengerCountValid(entry[fields['passenger_count']]),1)).reduceByKey(lambda x,y: x+y)
passcount_out = passenger_count.map(lambda x: x[0]+"\t"+str(x[1]))
passcount_out.saveAsTextFile("passenger_count_valid.out")

# Trip Distance
trip_distance = taxi_data.map(lambda entry: (checkTripDistanceValid(entry[fields['trip_distance']]),1)).reduceByKey(lambda x,y: x+y)
distance_out = trip_distance.map(lambda x: x[0]+"\t"+str(x[1]))
distance_out.saveAsTextFile("trip_distance_valid.out")

# Rate Code
rate_code = taxi_data.map(lambda entry: (checkRateCodeIdValid(entry[fields['RatecodeID']]),1)).reduceByKey(lambda x,y: x+y)
ratecode_out = rate_code.map(lambda x: x[0]+"\t"+str(x[1]))
ratecode_out.saveAsTextFile("RatecodeID_valid.out")

# Store and Fwd Flag
flag = taxi_data.map(lambda entry: (checkStoreAndFwdFlagValid(entry[fields['store_and_fwd_flag']]),1)).reduceByKey(lambda x,y: x+y)
flag_out = flag.map(lambda x: x[0]+"\t"+str(x[1]))
flag_out.saveAsTextFile("store_and_fwd_flag_valid.out")

# Payment Type
payment = taxi_data.map(lambda entry: (checkPaymentTypeValid(entry[fields['payment_type']]),1)).reduceByKey(lambda x,y: x+y)
payment_type_out = payment.map(lambda x: x[0]+"\t"+str(x[1]))
payment_type_out.saveAsTextFile("payment_type_valid.out")

# MTA tax
mta_tax = taxi_data.map(lambda entry: (checkMtaTaxValid(entry[fields['payment_type']]),1)).reduceByKey(lambda x,y: x+y)
mta_tax_out = mta_tax.map(lambda x: x[0]+"\t"+str(x[1]))
mta_tax_out.saveAsTextFile("mta_tax_valid.out")

# Surcharge
surcharge = taxi_data.map(lambda entry: (checkImprovementSurchargeValid(entry[fields['improvement_surcharge']]),1)).reduceByKey(lambda x,y: x+y)
surcharge_out = surcharge.map(lambda x: x[0]+"\t"+str(x[1]))
surcharge_out.saveAsTextFile("improvement_surcharge_valid.out")

# Paid amounts
amounts = ['fare_amount', 'tip_amount', 'tolls_amount', 'total_amount']
for amt in amounts:
	amount_ = taxi_data.map(lambda entry: (checkAmountValid(entry[fields[amt]]),1)).reduceByKey(lambda x,y: x+y)
	amount_out = amount_.map(lambda x: x[0]+"\t"+str(x[1]))
	amount_out.saveAsTextFile(amt + "_valid.out")

# Longitude
longitudes = ['pickup_longitude', 'dropoff_longitude']
for long in longitudes:
	longitude_ = taxi_data.map(lambda entry: (checkLongitude(entry[fields[long]]),1)).reduceByKey(lambda x,y: x+y)
	longitude_out = longitude_.map(lambda x: x[0]+"\t"+str(x[1]))
	longitude_out.saveAsTextFile(long + "_valid.out")

# Latitudes
latitudes = ['pickup_latitude', 'dropoff_latitude']
for lat in latitudes:
	latitude_ = taxi_data.map(lambda entry: (checkLatitude(entry[fields[lat]]),1)).reduceByKey(lambda x,y: x+y)
	latitude_out = latitude_.map(lambda x: x[0]+"\t"+str(x[1]))
	latitude_out.saveAsTextFile(lat + "_valid.out")

