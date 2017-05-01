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
with open('new_test.out','wb') as f:
	f.write('Counts\n')
	f.write('count: '+str(filtered.count())+'\n')
#filtered.saveAsTextFile('broadway.out')
