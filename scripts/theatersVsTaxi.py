from pyspark import SparkContext
sc=SparkContext()
import myUtils as my
import spatialUtils as sp
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")
sc.addPyFile("spatialUtils.py")

#data,flag = my.readAllFiles(sc)
data,flag =my.readFiles2(dict((y, [k for k in range(1,2)]) for y in range(2015,2016)),sc)
theaters = sp.loadNYCtheaters()
LON_LIM = 0.002
LAT_LIM = 0.0012 
ven = theaters['Apollo Theater']
box_fun = sp.createBox(ven,LON_LIM,LAT_LIM)

pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')
agg_data=aggregateOnDateTime(data,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(12)},
								days={'all_days':range(7)},
								hours={'all_hours':range(24)}).reduceByKey(lambda x,y: x+y)
with open('new_test.out','wb') as f:
	f.write('Counts\n')
	f.write('count: '+str(filtered.count())+'\n')
#filtered.saveAsTextFile('broadway.out')
