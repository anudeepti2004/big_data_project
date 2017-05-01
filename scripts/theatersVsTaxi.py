import myUtils as my
import spatialUtils as sp
data,flag = my.readAllFiles(sc)

my._fieldsDic['pickup_longitude']

theaters = sp.loadNYCtheaters()


LON_LIM = 0.002
LAT_LIM = 0.0012 
ven = theaters['Broadway Theatre']
box_fun = sp.createBox(ven,LON_LIM,LAT_LIM)

pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')

    

