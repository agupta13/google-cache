import os
import sys
import time
from geopy import geocoders 
from geopy import distance
from geopy.point import Point

summaryFile = 'summary-2014-04-26'
ixpFile = 'ip_city_country.pl'
ixp2proxyFile = 'ixp2proxy.txt'
distanceThreshold = 100

ixp2location = {}
location2proxy = {}
ixp2proxy = {}


def parse_IXPFile():
    with open(ixpFile) as f:
        for line in f:
            chunks = line.split('\t')
            if len(chunks)>1:
                [name, city, country] = chunks[2:5]
                location = city+", "+country
                ixp2location[name] = location


def parse_summaryFile():

    with open(summaryFile) as f:
        for line in f:
            [ip, asn, hostISP_name, lat, long, cntry] = line.split('\n')[0].split()
            proxy_location = (lat,long)
            if proxy_location not in location2proxy:
                location2proxy[proxy_location] = (asn,cntry,1)
            else:
                counter = location2proxy[proxy_location][2]+1
                location2proxy[proxy_location] = (asn,cntry,counter)
            

def find_proxies_near_ixps(g,d):
    for ixp in ixp2location:
        try:
            _, loc_ixp = g.geocode(ixp2location[ixp])
            print loc_ixp
            print "Analysing for IXP: ", ixp
            time.sleep(1)
            for proxy_location in location2proxy:               
                distance = d(loc_ixp, proxy_location).km
                if distance < distanceThreshold:
                    print "matched for the proxy", location2proxy[proxy_location]
                    if ixp not in ixp2proxy:
                        ixp2proxy[ixp]=[]                    
                    ixp2proxy[ixp].append((location2proxy[proxy_location],distance))
                    
        except:
            e = sys.exc_info()
            print e
            print "not able to process: ", ixp2location[ixp]
            
def store_result(ixp2proxy):
    print "Storing the processed result"
    f = open(ixp2proxyFile,'w')
    print ixp2proxy
    for ixp in ixp2proxy:
        print ixp, ixp2proxy[ixp]
        line = ""+ixp
        for proxy in ixp2proxy[ixp]:
            tmp = list(proxy[0])
            tmp = tmp + [int(proxy[1])]
            print tmp
            line += "|"+','.join(str(x) for x in tmp)
        line+='\n'
        f.write(line)
    f.close()
    
    
if __name__=='__main__':
    
    parse_IXPFile()    
    g = geocoders.GoogleV3()
    d = distance.distance 
    parse_summaryFile()
    
    find_proxies_near_ixps(g,d)
    
    #ixp2proxy={'1':[((12,'MA',2),10),((11,'MA',2),20)],'2':[((13,'KA',3),30)]}
    store_result(ixp2proxy)
    