#!/usr/bin/python
import sys
import multiprocessing
from multiprocessing import Pool
import numpy as np
from math import radians, sin, cos, asin, sqrt, pi, atan2
import heapq
import traceback
import logging
import itertools

#these nasty globals allow us to share data across processes
results = list()
haystack = None
values = None
ixp2locationFile = 'ixp2location.txt'
pfx2locationFile = 'prefix_lat_lon_country_asn_2014_04_14.txt'

num_process = 6
loc2ixp = {}
loc2pfx = {}
ixp2eyeballs = {}

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

def split(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def unique_ixpLocation():
    with open(ixp2locationFile) as f:
        count = 0
        for line in f:
            count += 1
            chunks = line.split('\n')[0].split('|')
            if chunks[2] not in loc2ixp:
                loc2ixp[chunks[2]] = []
            loc2ixp[chunks[2]].append((chunks[0], chunks[1]))
            #if count == 10:
            #    break
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
            #if count==1000:
            #    break
    print count, len(loc2pfx.keys())
    #print loc2pfx
    

def update_ixp2eyeballs(pfx_location2ixp):
    for elem in pfx_location2ixp:
        chunks = elem.split('|')
        ixps = chunks[1:]
        for ixp in ixps:
            tmp = ixp.split(',')
            if len(tmp) > 2:
                ixp_id = tmp[0]
                ixp_name = tmp[1]
                if (ixp_id, ixp_name) not in ixp2eyeballs:
                    ixp2eyeballs[(ixp_id, ixp_name)] = {}
                
                pfx_loc = chunks[0]
                for pfx_tuple in loc2pfx[pfx_loc]:
                    pfx_asn = pfx_tuple[4]
                    if pfx_asn == '*':
                        continue
                    if pfx_asn not in ixp2eyeballs[(ixp_id, ixp_name)]:
                        ixp2eyeballs[(ixp_id, ixp_name)][pfx_asn] = 1
                    else:
                        ixp2eyeballs[(ixp_id, ixp_name)][pfx_asn] += 1
            
           
    print len(pfx_location2ixp), len(ixp2eyeballs.keys())
        
        
def store_ixp2eyeballs():
    fout = open('ixp2eyeballs.txt','w')
    for (ixp_id,ixp_name) in ixp2eyeballs:
        line = ''+ixp_id+','+ixp_name
        for k,v in ixp2eyeballs[(ixp_id,ixp_name)].iteritems():
            line += '|'+k+','+str(v)
        line += '\n'
        fout.write(line)
    fout.close()
               

if __name__ == '__main__':
    unique_ixpLocation()
    unique_prefixes()
    
    
    multiprocessing.log_to_stderr(logging.ERROR)
    
    haystack = list()
    values = list()
    
    for loc_ixp in loc2ixp:
        
        [lat,lon] = loc_ixp.split(',')
        haystack.append((lat, lon))
        values.append(loc2ixp[loc_ixp])

    pool = Pool(num_process) #make this the number of cores to use!
    
    needles = list()
    prefixes = list()
    for prefix_loc in loc2pfx:
        [lat, lon] = prefix_loc.split(',')
        needles.append((lat,lon))
        prefixes.append(loc2pfx[prefix_loc])
        
    """
    create a list of lists of clients. each sub-list has 6000 clients. Each
    sub-list gets processed by one running process. 
    """
    needle_lists = list(split(needles, 60)) #convert generator into list
    prefix_lists = list(split(prefixes, 60)) 
    
    #these should be lists of lists
    assert len(needle_lists) == len(prefix_lists)
    
    for i in range(0, len(needle_lists)):
        needle_list = needle_lists[i]
        prefix_list = prefix_lists[i]
        pool.apply_async(process, (needle_list, prefix_list), callback=log_result)        

    pool.close()
    pool.join()
    
    #print results

    #merge list of lists into one list
    pfx_location2ixp = list(itertools.chain(*results))
    update_ixp2eyeballs(pfx_location2ixp)
    store_ixp2eyeballs()
    
    #distances_str = map(str, distances)
    out = '\n'.join(pfx_location2ixp)
    #print(out)
    #print ixp2eyeballs
    
    