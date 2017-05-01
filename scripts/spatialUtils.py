from __future__ import print_function
import sys
import myUtils
import re,random 

def loadNYCtheaters():
    theaters = {}
    with open('theaters.csv','r') as f:
        f.next()
        for l in f: 
            aa = l.split(',')
            coor = map(float,filter(lambda x: len(x)>0,re.split('[^\d.-]+',aa[0])))
            name = aa[1]
            theaters[name] = coor

    return theaters

def createBox(c_tuple,x_lim,y_lim):
# tuple of floats c_tuple,new_point as (lon,lat)
    box = ((c_tuple[0]-x_lim,c_tuple[1]-y_lim),(c_tuple[0]+x_lim,c_tuple[1]+y_lim))
    def filterBox(dd):
        for i in range(len(dd)):
            if not(box[0][i]<dd[i]<box[1][i]):
                return False
        return True
    return filterBox

def filterTheBox(data,filterBox,c_lon,c_lat):
    p_lon = myUtils._fieldsDic[c_lon]
    p_lat = myUtils._fieldsDic[c_lat]
    pickups = data.map(lambda a: map(float,[a[p_lon],a[p_lat]]))
    return pickups.filter(lambda a: filterBox(a))
