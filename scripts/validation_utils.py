from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext

def checkVendorIDValid(vendor_id):
	try:
        	int(vendor_id)
                if int(vendor_id) == 1 or int(vendor_id) == 2:
                	return (vendor_id,"Valid")
                else:
                	return (vendor_id,"Invalid")
	except ValueError:
                return (vendor_id,"Invalid")

def checkPickUpDateValid(date_text):
	if date_text is not None or date_text:
        	try:
        		given_date = datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
                        year = given_date.year
                        if year >= 2013 and year <= 2016:
                        	return "Valid"
                        else:
                                return "Invalid_year"
                   except ValueError:
                                return "Invalid_date"
                else:
                        return "Invalid_Null_date"

def checkDropoffDateValid(date_text):
	if date_text is not None:
        	try:
                	given_date = datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
                        year = given_date.year
                        if year >= 2013 and year <= 2017:
                        	return "Valid"
                        else:
                                return "Invalid_year"
                except ValueError:
                        return "Invalid_date"
	else:
        	return "Invalid_Null_date"

def checkPassengerCountValid(passenger_count):
                try:
                        int(passenger_count)
                        if int(passenger_count) >= 0 and int(passenger_count) < 10:
                                return "Valid"
                        else:
                                return "Invalid_not_within_range"
                except ValueError:
                        return "Invalid_not_integer"



def checkTripDistanceValid(trip_distance):
                try:
                        num = float(trip_distance)
                        if  num > 0:
                                return "Valid"
                        elif num == 0:
                                return "Invalid_ZeroTripDistance"
                        else:
                                return "Invalid_NegativeTripDistance"
                except ValueError:
                        return "Invalid_NotFloat"

def checkRateCodeIdValid(rateCodeId):
                try:
                        int(rateCodeId)
                        if int(rateCodeId) > 0 and int(rateCodeId) <= 6 :
                                return "Valid"
                        else:
                                return "Invalid_NotWithinRange"
                except ValueError:
                        return "Invalid_NotInteger"

def checkStoreAndFwdFlagValid(store_and_fwd_flag):
                if store_and_fwd_flag == 'Y' or store_and_fwd_flag == 'N':
                        return "Valid"
                else:
                        return "Invalid"

def checkPaymentTypeValid(payment_type):
                try:
                        int(payment_type)
                        if int(payment_type) > 0 and int(payment_type) <= 6 :
                                return "Valid"
                        else:
                                return "Invalid_NotWithinRange_"+payment_type
                except ValueError:
                        return "Invalid_NotInteger_"+payment_type


def checkMtaTaxValid(mta_tax):
        try:
            num = float(mta_tax)
            if  num == 0.5 :
                return "Valid"
            elif num == 0:
                return "Valid_ZeroTax"
            elif num < 0:
                return "Invalid_NegativeMtaTax"
            else:
                return "Invalid_not_0.5_or_0"
        except ValueError:
            return "Invalid_NotFloat"

def checkImprovementSurchargeValid(amount):
                if amount:
                        try:
                                num = float(amount)
				if  num ==  0.3:
                                        return "Valid"
                                elif num == 0:
                                        return "Valid_Zero_amount"
                                else:
                                        return "Invalid_Negative_amount"
                        except ValueError:
                                return "Invalid_NotFloat"
                else:
                        return "Invalid_Null"

#Check amount valid can be used for tolls, tip, total amount, fare and extra amount
def checkAmountValid(amount):
                if amount:
                        try:
                                num = float(amount)
                                if  num > 0:
                                        return "Valid"
                                elif num == 0:
                                        return "Valid_Zero_amount"
                                else:
                                        return "Invalid_Negative_amount"
                        except ValueError:
                                return "Invalid_NotFloat"
                else:
                        return "Invalid_Null"


