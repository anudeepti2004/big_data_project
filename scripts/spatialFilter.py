import myUtils as my
import re
data,flag = my.readAllFiles(sc)
my._fieldsDic['tpep_pickup_datetime']

theaters = {}
with open('theaters.csv','r') as f:
    f.next()
    for l in f: 
        aa = l.split(',')
        coor = map(float,filter(lambda x: len(x)>0,re.split('[^\d.-]+',aa[0])))
        name = aa[1]
        theaters[name] = coor


LON_LIM = 0.002
LAT_LIM = 0.0012 

ven = theaters['Broadway Theatre']
box = my.createBox(ven,LON_LIM,LAT_LIM)
filtered = my.filterTheBox(data,box,'pickup_longitude','pickup_latitude')

    

