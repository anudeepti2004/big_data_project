import myUtils as my
import spatialUtils as sp
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")
sc.addPyFile("spatialUtils.py")

#data,flag = my.readAllFiles(sc)
y_m_dic = dict((y, [k for k in range(1,2)]) for y in range(2015,2017))
data,flag =myreadFiles2(y_m_dic,sc)
theaters = sp.loadNYCtheaters()
LON_LIM = 0.002
LAT_LIM = 0.0012 
ven = theaters['Broadway Theatre']
box_fun = sp.createBox(ven,LON_LIM,LAT_LIM)

pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')
filtered.count()
    

