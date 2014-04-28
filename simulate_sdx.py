import os
import sys
import time
import json
import math
import sqlite3 as lite
import multiprocessing
from multiprocessing import Pool
import numpy as np
from math import radians, sin, cos, asin, sqrt, pi, atan2
import heapq
import traceback
import logging
import itertools

from geopy import geocoders 
from geopy import distance

import proxyIXPClustering as pxyixp
import pdbParser as pdbParser
#import closest_IXP as closest

dbFile = 'peeringdb_dump_2014_04_28.sqlite'
table1 = 'peerParticipantsPublics'
table2 = 'mgmtPublics'
table3 = 'peerParticipants'

ixp2locationFile = 'ixp2location.txt'
summaryFile = 'bigsummary_2014-04-14'
ixp2proxyFile = 'ixp2proxy.dat'
ixp2proxy_updatedFile = 'ixp2proxy_updated.dat'
participant2policyFile = 'participant2policy.dat'
ixp2participantsFile = 'ixp2participants.dat'
pfx2locationFile = 'prefix_lat_lon_country_asn_2014_04_14.txt'
ixp2customersFile = 'ixp2customers.dat'
pfx2ixpFile = 'pfx2ixp.dat'
pfx2ixp_updatedFile = 'pfx2ixp_updated.dat'


distanceThreshold = 500 # km
ixp2location = {}
location2proxy = {}
ixp2proxy = {}
participant2policy = {}
ixp2participants = {}
loc2ixp = {}
loc2pfx = {}

#these nasty globals allow us to share data across processes
results = list()
haystack = None
values = None
num_process = 6


def fix_asn(asn):
    if '_' in asn:
        asn = asn.split('_')[0]
    if '.' in asn:
        asn = asn.split('.')[0]
    if '{' in asn:
        asn = asn.split('{')[1].split('}')[0]
    if '*' in asn:
        asn = -1
    
    return asn


def is_connected(isp_asn, ixp_id):
    if int(ixp_id) in ixp2participants:
        _, participants = ixp2participants[int(ixp_id)]
        if int(isp_asn) in participants:
            return True
        else:
            return False
    else:
        return False  


def get_policy(isp_asn):
    if math.ceil(float(isp_asn)) in participant2policy:
        return participant2policy[math.ceil(float(isp_asn))]
    else:
        return "NA"
    
    
def parse_IXP2Location():
    with open(ixp2locationFile) as f:
        for line in f:
            chunks = line.split('\n')[0].split('|')
            #print chunks
            loc = tuple(chunks[2].split(','))
            id = chunks[0]
            name = chunks[1]
            ixp2location[id] = (name,loc)
    return ixp2location


def unique_ixpLocation():
    with open(ixp2locationFile) as f:
        count = 0
        for line in f:
            count += 1
            chunks = line.split('\n')[0].split('|')
            if chunks[2] not in loc2ixp:
                loc2ixp[chunks[2]] = []
            loc2ixp[chunks[2]].append((chunks[0], chunks[1]))
            if count == 10:
                break
    print count, len(loc2ixp.keys())
    #print loc2ixp


def unique_prefixes():
    with open(pfx2locationFile) as f:
        count = 0
        for line in f:
            count += 1
            chunks = line.split('\n')[0].split(' ')
            #print chunks
            loc = chunks[1]+','+chunks[2]
            if loc not in loc2pfx:
                loc2pfx[loc] = []
            loc2pfx[loc].append(tuple(chunks))
            if count==1000:
                break
    print count, len(loc2pfx.keys())
    #print loc2pfx
    
def dist_vector(needle, haystack):
    """
    Calculate the distance from needle to all points in haystack
    """
    lats = [float(x[0]) for x in haystack]
    lons = [float(x[1]) for x in haystack]
    #print lats, [needle[0]]
    
    #needle[0] = float(needle[0])
    #needle[1] = float(needle[1])

    dlat = np.radians(lats) - radians(float(needle[0]))
    #print dlat
    dlon = np.radians(lons) - radians(float(needle[1]))
    a = np.square(np.sin(dlat/2.0)) + cos(radians(float(needle[0]))) * np.cos(np.radians(lats)) * np.square(np.sin(dlon/2.0))
    great_circle_distance = 2 * np.arcsin(np.minimum(np.sqrt(a), np.repeat(1, len(a))))
    d = 6367 * great_circle_distance  #vector of distances
    return d

def closest(needle, haystack, values):
    d = dist_vector(needle, haystack)
    i = np.argmin(d)          #index of minimum distance
    return (values[i], d[i])  #return tuple with distance and matching ip

def log_result(result):
    results.append(result)
    sys.stderr.write('results: %d\n' % len(results))
    sys.stderr.flush()

def process(needle_list, prefix_list):

    try:
        distances = list()
        
        #for needle in needle_list:
        for i in range(0, len(needle_list)):
            needle = needle_list[i]
            prefix = prefix_list[i]

            dist = closest(needle, haystack, values)
            #print type(dist[0])
            #print prefix
            str = ''+','.join(list(needle))+'|'
            for elem in dist[0]:
                str += ','.join(list(elem)) 
                str += ',' + '%.0f' % dist[1]
                #str += ','+str(float(dist[1])
                str += '|'

            distances.append(str)
        return distances
    except:
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()


def get_location2proxy():
    with open(summaryFile) as f:
        for line in f:
            [ip, asn, hostISP_name, lat, long, cntry] = line.split('\n')[0].split()
            proxy_location = (lat,long)
            if proxy_location not in location2proxy:
                location2proxy[proxy_location] = [asn]
            else:
                if asn not in location2proxy[proxy_location]:
                    location2proxy[proxy_location].append(asn)


def get_ixp2proxy_thresh(thresh,d):    
    for ixp_id in ixp2location:
        try:
            name, loc_ixp = ixp2location[ixp_id]
            print name, loc_ixp
            print "Analysing for IXP: ", name
            for proxy_location in location2proxy: 
                             
                distance = d(loc_ixp, proxy_location).km
                #print "proxy_location", proxy_location, "loc_ixp: ",loc_ixp, distance
                if distance < thresh:
                    print "matched for the proxy", location2proxy[proxy_location]
                    if ixp_id not in ixp2proxy:
                        ixp2proxy[ixp_id]=[name]
                    for asn in location2proxy[proxy_location]:                                      
                        ixp2proxy[ixp_id].append((asn,int(distance)))
                    
        except:
            e = sys.exc_info()
            print e
            print "not able to process: ", ixp2location[ixp_id]    


def get_ixp2participants(con):
    with con:
        cur = con.cursor()
        with open(ixp2locationFile) as f:
            for line in f:
                chunks = line.split('\n')[0].split('|')
                ixp_id = str(chunks[0])
                ixp_name = chunks[1]
                #print "Trying to locate ixp: ",ixp_name
                command = "SELECT local_asn FROM "+table1+" WHERE public_id='"+ixp_id+"'"
                cur.execute(command)
                data = cur.fetchall()
                participants = []
                for elem in data:
                    elem = int(elem[0])
                    if elem not in participants:
                        participants.append(elem)

                ixp2participants[int(ixp_id)] = (ixp_name,participants) 
    
    with open(ixp2participantsFile, 'w') as outfile:
        json.dump(ixp2participants, outfile, ensure_ascii=True, encoding="ascii")
   

def split(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def get_participants2policy(con):
    with con:
        cur = con.cursor()
        command = "SELECT asn, policy_general FROM "+table3
        cur.execute(command)
        data = cur.fetchall()
        for elem in data:
            asn, policy = elem
            if asn!=None:
                participant2policy[int(asn)] = str(policy)
                
    with open(participant2policyFile, 'w') as outfile:
        json.dump(participant2policy, outfile, ensure_ascii=True, encoding="ascii")
    
    
def get_ixp2proxy():
    parse_IXP2Location()
    get_location2proxy()
    #print len(location2proxy.keys())    
    
    thresh = distanceThreshold
    d = distance.distance 
    get_ixp2proxy_thresh(thresh,d)
    print len(ixp2proxy.keys())
    with open(ixp2proxyFile, 'w') as outfile:
        json.dump(ixp2proxy,outfile,ensure_ascii=True,encoding="ascii")
       
    
def update_ixp2proxy():
    ixp2proxy = json.load(open(ixp2proxyFile, 'r'))
    tmp = {}
    for ixp_id in ixp2proxy:
        ixp_name= str(ixp2proxy[ixp_id][0])
        isps = ixp2proxy[ixp_id][1:]
        key = ','.join([ixp_id,ixp_name])
        tmp[key] = {}
        
        for isp in isps:
            isp_asn = str(isp[0])
            distance = int(isp[1])
            
            isp_asn = fix_asn(isp_asn)
            ixp_connected = is_connected(isp_asn,ixp_id)
            if ixp_connected:
                peering_policy = get_policy(isp_asn)
            else:
                peering_policy = 'not-connected'
            if isp_asn not in tmp[key]:
                tmp[key][isp_asn] = (distance, peering_policy)
            else:
                cur_distance = tmp[key][isp_asn][0]
                # consider the shortest distance in case multiple proxies are hosted by the same ISP
                if distance < cur_distance:
                    tmp[key][isp_asn] = (distance, tmp[key][isp_asn][1])
        #print tmp[ixp_id]
        
    ixp2proxy_updated = tmp
    with open(ixp2proxy_updatedFile, 'w') as outfile:
        json.dump(ixp2proxy_updated, outfile, ensure_ascii=True, encoding="ascii")
    print len(ixp2proxy_updated.keys())
                
        
def filter_ixp2proxy():
    """
       If an AS has two frontend locations nearby an IXP, we'll consider the nearest one.
       Currently we are considering the case where hosting ISP is direct connected at the IXP.
       We can later extend this work where hosting ISP's provider is present at the IXP. 
    """
    con = lite.connect(dbFile)
    get_ixp2participants(con)
    get_participants2policy(con)
    update_ixp2proxy()

    
def update_pfx2ixp():
    ixp2customer = json.load(open(ixp2customersFile,'r'))
    participant2policy = json.load(open(participant2policyFile,'r'))
    pfx2ixp = json.load(open(pfx2ixpFile,'r'))
    pfx2ixp_updated = {}
    
    for prefix_info in pfx2ixp:
        pfx2ixp_updated[prefix_info] = {}        
        pfx_asn = prefix_info.split(',')[1]
        for ixp_info in pfx2ixp[prefix_info]:
            asn = ''
            policy = ''
            distance = str(pfx2ixp[prefix_info][ixp_info])
            #print pfx_asn, ixp2customer[ixp_info]
            if pfx_asn in ixp2customer[ixp_info]:
                #print "case 1"
                # this prefix's as is itself participant at IXP
                asn = pfx_asn
                #print participant2policy.keys()[0]
                if asn in participant2policy:
                    policy = participant2policy[asn] 
                else:
                    policy = 'NA' 
                #print [asn, policy, distance]                              
            else:
                # check if prefix's AS is customer to IXP's participants
                for ixp_parts in ixp2customer[ixp_info]:
                    if pfx_asn in ixp2customer[ixp_info][ixp_parts]:
                        #print "case 2"
                        asn = ixp_parts
                        if asn in participant2policy:
                            policy = participant2policy[asn] 
                        else:
                            policy = 'NA' 
                        #policy = participant2policy[asn]
                        #print [asn, policy, distance]
                        break
            if asn != '':
                # Consider only the cases where we had a match
                pfx2ixp_updated[prefix_info][ixp_info] = ','.join([asn, policy, distance])
                
            #pfx2ixp[prefix_info][ixp_info] = ','.join([asn, policy, distance])
    #print pfx2ixp_updated
    with open(pfx2ixp_updatedFile, 'w') as outfile:
        json.dump(pfx2ixp_updated, outfile, ensure_ascii=True, encoding="ascii")
                        
    
                
                
    
def simulate_sdx():
    #get_ixp2proxy()
    #filter_ixp2proxy()
    update_pfx2ixp()
    

    

if __name__ == '__main__':
    simulate_sdx()
