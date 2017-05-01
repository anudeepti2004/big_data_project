import myUtils as my
import spatialUtils as sp
data,flag = my.readAllFiles(sc)
my._fieldsDic['tpep_pickup_datetime']

theaters = sp.loadNYCtheaters()


LON_LIM = 0.002
LAT_LIM = 0.0012 

ven = theaters['Broadway Theatre']
box_fun = sp.createBox(ven,LON_LIM,LAT_LIM)
filtered = sp.filterTheBox(data,box_fun,'pickup_longitude','pickup_latitude')

    

