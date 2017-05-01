import myUtils as my
import spatialUtils as sp
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")
sc.addPyFile("spatialUtils.py")

#data,flag = my.readAllFiles(sc)
y_m_dic = dict((y, [k for k in range(1,2)]) for y in range(2015,2016))
data,flag =my.readFiles2(y_m_dic,sc)
theaters = sp.loadNYCtheaters()
LON_LIM = 0.002
LAT_LIM = 0.0012 
ven = theaters['Broadway Theatre']
box_fun = sp.createBox(ven,LON_LIM,LAT_LIM)

pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')
filtered.count()
    

c_lon,c_lat='pickup_longitude','pickup_latitude'
p_lon = my._fieldsDic[c_lon]
p_lat = my._fieldsDic[c_lat]
pickups = data.map(lambda a: map(float,[a[p_lon],a[p_lat]]))