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
from statistics import *


import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from scipy.stats import cumfreq
import pylab as pl
import numpy as np
from statistics import *
from matplotlib.ticker import MaxNLocator
my_locator = MaxNLocator(6)

#import proxyIXPClustering as pxyixp
#import pdbParser as pdbParser
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
ixp2proxy_nearestFile = 'ixp2proxy_nearest.dat'
pfx2proxy_nearestFile = 'pfx2proxy_nearest.dat'
pfx2proxy_distancesFile = 'pfx2proxy_distances.dat'
edgecastprefixFile = 'edgecast_prefix_summary_full_filtered.txt'
pfx2requestsFile = 'pfx2requests.dat'


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
        if pfx2ixp_updated[prefix_info] == {}:
            pfx2ixp_updated.pop(prefix_info,None)
    print len(pfx2ixp_updated.keys())
    with open(pfx2ixp_updatedFile, 'w') as outfile:
        json.dump(pfx2ixp_updated, outfile, ensure_ascii=True, encoding="ascii")


def get_ixp2proxy_nearest():
    ixp2proxy_updated = json.load(open(ixp2proxy_updatedFile,'r'))
    ixp2proxy_nearest = {}
    for k1,v1 in ixp2proxy_updated.iteritems():

        tmp1 = [] # distances to proxies with "open" policies
        tmp2 = [] # distances to all other connected proxies
        for k2,v2 in v1.iteritems():
            distance = int(v2[0])
            policy = v2[1].lower()
            if policy != 'not-connected':
                tmp2.append(distance)
                if policy == "open":
                    tmp1.append(distance)
        tmp1.sort()
        tmp2.sort()

        if len(tmp1) > 0:
            min_open = tmp1[0]
        else:
            min_open = -1
        if len(tmp2) > 0:
            min_sdx = tmp2[0]
        else:
            min_sdx = -1

        ixp2proxy_nearest[k1] = [min_open, min_sdx]

    print ixp2proxy_nearest

    with open(ixp2proxy_nearestFile, 'w') as outfile:
        json.dump(ixp2proxy_nearest, outfile, ensure_ascii=True, encoding="ascii")


def get_pfx2proxy_nearest():
    ixp2proxy_nearest = json.load(open(ixp2proxy_nearestFile,'r'))
    pfx2ixp_updated = json.load(open(pfx2ixp_updatedFile,'r'))
    pfx2proxy_nearest = {}
    for k1, v1 in  pfx2ixp_updated.iteritems():
        tmp1 = []
        tmp2 = []
        #print "processing for the pfx: ",k1
        for k2, v2 in v1.iteritems():
            chunks = v2.split(',')
            # distance from proxy to IXP
            distance1 = int(chunks[2])
            policy = chunks[1].lower()
            if k2 in ixp2proxy_nearest:
                #print " processing for IXP: ", k2, ixp2proxy_nearest[k2], distance1
                if policy == 'open':
                    # Get distance from IXP to proxy
                    distance_open = int(ixp2proxy_nearest[k2][0])

                    if distance_open >= 0:
                        tmp1.append(distance1 + distance_open)
                distance_sdx = int(ixp2proxy_nearest[k2][1])
                if distance_sdx >= 0:
                    tmp2.append(distance1 + distance_sdx)

        tmp1.sort()
        tmp2.sort()
        #print tmp1, tmp2

        if len(tmp1) > 0:
            min_open = tmp1[0]
        else:
            min_open = -1

        if len(tmp2) > 0:
            min_sdx = tmp2[0]
        else:
            min_sdx = -1

        pfx2proxy_nearest[k1] = [min_open, min_sdx]
        #print pfx2proxy_nearest[k1]

    print pfx2proxy_nearest

    with open(pfx2proxy_nearestFile, 'w') as outfile:
        json.dump(pfx2proxy_nearest, outfile, ensure_ascii=True, encoding="ascii")


def get_cdf(elem):
    num_bins=10000
    counts, bin_edges = np.histogram(elem,bins=num_bins,normed=True)
    #print bin_edges
    cdf=np.cumsum(counts)
    scale = 1.0/cdf[-1]
    cdf=cdf*scale
    return bin_edges[1:], cdf


def plot_pfx2proxy():

    pfx2proxy_distances = json.load(open(pfx2proxy_distancesFile,'r'))
    data = pfx2proxy_distances.values()
    legends = pfx2proxy_distances.keys()
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    color_n=['g','r','b','m','c','k','w']
    markers=['o','*','^','s','d','3','d','o','*','^','1','4']
    linestyles=[ '--',':','-','-.']

    i =0
    plots = []
    for elem in data:
        x, y = get_cdf(elem)
        plots.append(pl.plot(x,y,label=legends[i],color=color_n[i],linestyle=linestyles[i]))
        i += 1
    plots = [x[0] for x in plots]
    pl.legend((plots),legends,'lower right')
    pl.xlabel('Distance (km)')

    pl.ylabel('CDF')
    ax.set_ylim(ymin=0.01)

    ax.grid(True)
    plt.tight_layout()
    plot_name='pfx2proxy'+'.eps'
    plot_name_png='pfx2proxy'+'.png'
    pl.savefig(plot_name)
    pl.savefig(plot_name_png)





def process_pfx2proxy_nearest():
    pfx2proxy_nearest = json.load(open(pfx2proxy_nearestFile,'r'))
    distances_open = []
    distances_sdx = []
    for k, v in pfx2proxy_nearest.iteritems():
        distances_open.append(int(v[0]))
        distances_sdx.append(int(v[1]))
    print "# of prefixes into consideration: ", len(distances_sdx)
    distances_open = filter(lambda x: x >=0, distances_open)
    total, average, median, standard_deviation, minimum, maximum, confidence = stats(distances_open,
                                                                                     confidence_interval=0.05)
    print "# of prefixes crossing IXP with existing OPEN policies: ", len(distances_open), "median: ", median
    distances_sdx = filter(lambda x: x >=0, distances_sdx)
    total, average, median, standard_deviation, minimum, maximum, confidence = stats(distances_sdx,
                                                                                     confidence_interval=0.05)

    print "# of prefixes crossing IXP with existing SDX policies:  ", len(distances_sdx), "median: ", median
    pfx2proxy_distances = {}
    pfx2proxy_distances['SDX-SDX'] = distances_sdx
    pfx2proxy_distances['Open-Open'] = distances_open

    with open(pfx2proxy_distancesFile, 'w') as outfile:
        json.dump(pfx2proxy_distances, outfile, ensure_ascii=True, encoding="ascii")


def get_pfx2requests():
    pfx2requests = {}
    with open(edgecastprefixFile) as f:
        counter = 0
        for line in f:
            chunks = line.split('\n')[0].split(' ')
            if len(chunks) == 5:
                prefix_key = ','.join([chunks[0],chunks[3]])
                queries = int(chunks[4])
                pfx2requests[prefix_key] = queries

    with open(pfx2requestsFile, 'w') as outfile:
        json.dump(pfx2requests, outfile, ensure_ascii=True, encoding="ascii")


def get_pfx2gfe():
    # prefix to google front end
    pfx2gfe = {}
    with open('prefix_closest_googlehosted.txt') as f:
        counter = 0
        for line in f:
            counter += 1
            chunks = line.strip().split(' ')
            prefix = chunks[0]
            distance = int(float(chunks[3]))
            pfx2gfe[prefix] = distance
            #if counter == 10000:
            #    break

        with open('pfx2gfe.dat', 'w') as outfile:
            json.dump(pfx2gfe, outfile, ensure_ascii = True, encoding = "ascii")


def get_pfx2ispfe():
    # prefix to closest provider hosted isp
    pfx2ispfe = {}
    with open('prefixes-closest-providers.txt') as f:
        counter = 0
        for line in f:
            counter += 1
            chunks = line.strip().split(' ')
            prefix = chunks[0]
            distance = int(float(chunks[3]))
            pfx2ispfe[prefix] = distance
            #if counter == 10000:
            #    break

        with open('pfx2ispfe.dat', 'w') as outfile:
            json.dump(pfx2ispfe, outfile, ensure_ascii = True, encoding = "ascii")


def get_pfx2proxy_default():
    # set X is set(edgecast prefixes) - set(prefixes within on-net) - set(prefixes within off-net)
    pfx2requests = json.load(open(pfx2requestsFile,'r'))
    pfx2ispfe = json.load(open('pfx2ispfe.dat', 'r'))
    pfx2gfe = json.load(open('pfx2gfe.dat', 'r'))

    list1 = []
    list2 = []
    Xlist = []
    count = 0
    print "# of prefixes from edgecast: ", len(pfx2requests.keys())
    for pfx_info in pfx2requests:
        count +=1
        chunks = pfx_info.split(',')

        [prefix, asn] = chunks[:2]
        #[prefix, asn] = pfx_info.split(',')
        Xlist.append(prefix)
        if prefix in pfx2gfe:
            distance1 = int(pfx2gfe[prefix])
            if distance1 <= distanceThreshold:
                list1.append(prefix)

        if prefix in pfx2ispfe:
            distance2 = int(pfx2ispfe[prefix])
            if distance2 <= distanceThreshold:
                list2.append(prefix)
        #if count == 1000:
        #    break

    print "Starting the set difference operation", len(Xlist), len(list1),len(list2)
    X = set(Xlist)
    X = X.difference(set(list1))
    X = X.difference(set(list2))
    X = list(X)
    print " # of prefixes which can possibly benefit from IXPs/SDX: ", len(X)
    pfx2proxy_default = {}
    count = 0
    for prefix in X:
        distance1 = 10*distanceThreshold
        distance2 = 10*distanceThreshold
        if prefix in pfx2gfe:
            distance1 = int(pfx2gfe[prefix])
        if prefix in pfx2ispfe:
            distance2 = int(pfx2ispfe[prefix])
        distance = min([distance1, distance2])
        if distance == 10*distanceThreshold:
            distance = -1
            count += 1
        pfx2proxy_default[prefix] = distance

    print count
    print "Dumping the data"
    with open('pfx2proxy_default.dat', 'w') as outfile:
        json.dump(pfx2proxy_default, outfile, ensure_ascii=True, encoding="ascii")


def remove_prefixs_asn(dict):
    out = {}
    for k, v in dict.iteritems():
        k1 = k.split(',')[0]
        out[k1] = v
    return out

def merge_edgecast_allprefixes():
    pfx2proxy_default = json.load(open('pfx2proxy_default.dat','r'))
    pfx2proxy_nearest = json.load(open(pfx2proxy_nearestFile,'r'))
    pfx2proxy_nearest = remove_prefixs_asn(pfx2proxy_nearest)
    print "Sanity check"
    print " default: ", len(pfx2proxy_default.keys())," nearest: ", len(pfx2proxy_nearest.keys())
    set1 = set(pfx2proxy_default.keys())
    set2 = set(pfx2proxy_nearest.keys())
    set3 = set1.intersection(set2)
    print len(set3)
    print pfx2proxy_nearest.keys()
    return 0

    pfx2proxy_remaining = {}
    for prefix in pfx2proxy_default:
        #print prefix
        if prefix in pfx2proxy_nearest:
            distances = pfx2proxy_nearest[prefix]
            distances.append(pfx2proxy_default[prefix])
            #if sum(distances) != -3:
            #    # Do not add if none of the case we considered is able to reach the proxy
            pfx2proxy_remaining[prefix] = distances

    print " After merging the two datasets, remaining", len(pfx2proxy_remaining.keys())

    print "dumping the data"
    with open('pfx2proxy_remaining.dat', 'w') as outfile:
        json.dump(pfx2proxy_remaining, outfile, ensure_ascii=True, encoding="ascii")


def compare_edgecast_allprefixes():

    pfx2location = {}
    with open(pfx2locationFile) as f:
        for line in f:
            chunks = line.strip().split(' ')
            pfx2location[unicode(chunks[0])] = ''

    print pfx2location.keys()[:10]

    pfx2requests = json.load(open(pfx2requestsFile,'r'))
    pfx2requests = remove_prefixs_asn(pfx2requests)
    print pfx2requests.keys()[:10]
    set1 = set(pfx2requests.keys())
    set2 = set(pfx2location.keys())
    set3 = set.intersection(set1, set2)
    print len(set3)
    pfx2proxy_nearest = json.load(open(pfx2proxy_nearestFile,'r'))
    pfx2proxy_nearest = remove_prefixs_asn(pfx2proxy_nearest)

    pfx2requests_filtered = {}
    for prefix in list(set3):
        pfx2requests_filtered[prefix] = pfx2requests[prefix]

    with open('pfx2requests_filtered.dat', 'w') as outfile:
        json.dump(pfx2requests_filtered, outfile, ensure_ascii=True, encoding="ascii")

    print len(pfx2requests_filtered.keys())
    """
    set4 = set(pfx2proxy_nearest.keys())
    #set5 = set.intersection(set3, set4)
    print "# of prefixes near IXPs with location data: ", len(set4)
    #print len(set5)
    #pfx2proxy_default = json.load(open('pfx2proxy_default.dat','r'))
    #set6 = set(pfx2proxy_default.keys())
    #set7 = set.intersection(set5, set6)
    #print "# of prefixes which can benefit from new peerings", len(set7)

    pfx2ispfe = json.load(open('pfx2ispfe.dat', 'r'))
    pfx2ispfe = remove_prefixs_asn(pfx2ispfe)
    pfx2gfe = json.load(open('pfx2gfe.dat', 'r'))
    pfx2gfe = remove_prefixs_asn(pfx2gfe)


    pfx2proxy_filtered = {}
    for prefix in list(set5):
        distance1 = 10*distanceThreshold
        distance2 = 10*distanceThreshold
        if prefix in pfx2gfe:
            distance1 = int(pfx2gfe[prefix])
        if prefix in pfx2ispfe:
            distance2 = int(pfx2ispfe[prefix])
        distance_default = min([distance1, distance2])
        if distance_default == 10*distanceThreshold:
            distance_default = -1
        pfx2proxy_filtered[prefix] = pfx2proxy_nearest[prefix]
        pfx2proxy_filtered[prefix].append(distance_default)
    print "Updated the pfx2proxy result for filteredt prefix, # = ",len(pfx2proxy_filtered.keys())
    with open('pfx2proxy_filtered.dat', 'w') as outfile:
            json.dump(pfx2proxy_filtered, outfile, ensure_ascii=True, encoding="ascii")


    list1 = []
    list2 = []
    for prefix in pfx2gfe:
        distance1 = int(pfx2gfe[prefix])
        if distance1 <= distanceThreshold:
            list1.append(prefix)
    for prefix in pfx2ispfe:
        distance2 = int(pfx2ispfe[prefix])
        if distance2 <= distanceThreshold:
            list2.append(prefix)
    print len(set(list2)), len(set(list1))
    set8 = set.union(set(list1),set(list2))
    print len(set8)
    #set9 = set4-set8
    #print "Difference # of prefixes which can benefit from new peerings", len(set9)
    set10 = set.intersection(set3, set8)
    print "total edgecast: ", len(set3), " benefited: ", len(set10), "not benefited", len(set3)-len(set10)
    set11 = (set(list2)-set(list1))
    print len(set11)
    set12 = set.intersection(set11, set3)
    print len(set12)
    """

def get_pfx2proxy_redirected():
    pfx2proxy_redirected = {}
    with open('prefixes-default.txt') as f:
        for line in f:
            chunks = line.strip().split(' ')
            if len(chunks) == 4:
                #print chunks
                pfx2proxy_redirected[chunks[0]] = int(float(chunks[3]))

    with open('pfx2proxy_redirected.dat', 'w') as outfile:
        json.dump(pfx2proxy_redirected, outfile, ensure_ascii=True, encoding="ascii")


def get_pfx2proxy_triplet():
    pfx2proxy_nearest = json.load(open(pfx2proxy_nearestFile,'r'))
    pfx2proxy_nearest = remove_prefixs_asn(pfx2proxy_nearest)
    pfx2proxy_redirected = json.load(open('pfx2proxy_redirected.dat','r'))
    pfx2requests_filtered = json.load(open('pfx2requests_filtered.dat','r'))

    print "loaded all the required data structures, # pfx2requests_filtered ", len(pfx2requests_filtered.keys())
    pfx2proxy_triplet = {}
    for prefix in pfx2requests_filtered:
        # We'll have distance triplet for all the edgecast prefixes with location data.
        distance_triplet = [-1, -1]
        if prefix in pfx2proxy_nearest:

            distance_triplet = pfx2proxy_nearest[prefix]
            #print distance_triplet
        #else:
        #    distance_dual = [-1,-1]
        if prefix in pfx2proxy_redirected:

            redirected_distance = int(pfx2proxy_redirected[prefix])
            distance_triplet.append(redirected_distance)
            #print distance_triplet
            pfx2proxy_triplet[prefix] = distance_triplet

    print "dumping the data len: ", len(pfx2proxy_triplet.keys())

    with open('pfx2proxy_triplet.dat','w') as outfile:
        json.dump(pfx2proxy_triplet, outfile, ensure_ascii=True, encoding="ascii")


def convert_prefixes_to_requests(closest_to_proxy_prefixes):
    closest_to_proxy_queries = {0: 0, 1: 0, 2: 0}
    total_queries = 0
    pfx2requests_filtered = json.load(open('pfx2requests_filtered.dat','r'))
    for k, v in closest_to_proxy_prefixes.iteritems():
        for prefix in v:
            nqueries = int(pfx2requests_filtered[prefix])
            closest_to_proxy_queries[k] += nqueries
            total_queries += nqueries
            
    return closest_to_proxy_queries, total_queries
            

def plot_cdf(input_data, legends, labels, figname):
    data = input_data
    legends = legends
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    color_n=['g','r','b','m','c','k','w']
    markers=['o','*','^','s','d','3','d','o','*','^','1','4']
    linestyles=[ '--',':','-','-.']

    i =0
    plots = []
    for elem in data:
        x, y = get_cdf(elem)
        if len(legends) > 0:
            plots.append(pl.plot(x, y,label=legends[i],color=color_n[i],linestyle=linestyles[i]))
        else:
            plots.append(pl.plot(x,y,color=color_n[i],linestyle=linestyles[i]))
        i += 1
    if len(legends) > 0:
        plots = [x[0] for x in plots]
        pl.legend((plots),legends,'lower right')
        pl.xlabel(labels[0])

    pl.ylabel(labels[1])
    ax.set_ylim(ymin=0.01)

    ax.grid(True)
    plt.tight_layout()
    plot_name = figname+'.eps'
    plot_name_png = figname+'.png'
    pl.savefig(plot_name)
    pl.savefig(plot_name_png)
       
    

def analyze_pfx2proxy_triplet():
    pfx2proxy_triplet = json.load(open('pfx2proxy_triplet.dat','r'))
    
    print "loaded the required data structures"
    total =  len(pfx2proxy_triplet.keys())
    print "Total edgecast prefixes considered: ", total
    closest_to_proxy = {0: 0, 1: 0, 2: 0}
    closest_to_proxy_prefixes = {0: [], 1: [], 2: []}
    
    improvement_distance = []
    distance_distribution_improvement = {0: [], 1: [], 2: []}
    for k, v in pfx2proxy_triplet.iteritems():
        
        v = [(100*distanceThreshold) if x == -1 else x for x in v]
        # Since it returns the first value, we bias in favor of Open peering.
        # This is ok if v[0] != v[2]
        
        min_index = v.index(min(v))
        if (min_index < 2) and (v[min_index] == v[2]):
            closest_to_proxy[2] +=1
            closest_to_proxy_prefixes[2].append(k)
        else:
            """ This is the case when links at IXPs can bring client closer to FEs"""
            closest_to_proxy[min_index] +=1
            closest_to_proxy_prefixes[min_index].append(k)
            
        if min_index < 2:
            if v[2] != 100*distanceThreshold:
                diff = v[2] - v[min_index]
                if diff > 0 and diff < 10000:
                    improvement_distance.append(diff)
                    ind = 0
                    for elem in v:
                        if elem != 100*distanceThreshold:
                            if elem < 10000:
                                distance_distribution_improvement[ind].append(int(elem))
                        ind +=1
    
    print "plot distances cdf"
    legends = ['Additional Peering Links', 'Existing Peering Links', 'Redirected'] 
    input_data = distance_distribution_improvement.values()
    figname = "peering_links_improvements_distances"
    labels = ['Distance (km)', 'CDF'] 
    plot_cdf(input_data, legends, labels, figname) 
    
    legends = [] 
    input_data = [improvement_distance]
    figname = "peering_links_distance_closer"
    labels = ['Distance (km)', 'CDF'] 
    plot_cdf(input_data, legends, labels, figname)      
            
    
    total, average, median, standard_deviation, minimum, maximum, confidence = stats(improvement_distance)
    print "Frontend gets closer for ", len(improvement_distance), " prefixes"
    print "Improvements in distance, median: ", median, " maximum: ", maximum  
        
    print "Distribution of closest path to frontend" 
    print "# Prefixes -- redirected: ", closest_to_proxy[2], " open: ", closest_to_proxy[0], " SDX: ", closest_to_proxy[1]
    
    print len(distance_distribution_improvement[0]), len(distance_distribution_improvement[1]), len(distance_distribution_improvement[2])
       
    print " Calling prefix to query converter"
    closest_to_proxy_queries, total_queries = convert_prefixes_to_requests(closest_to_proxy_prefixes) 
    print " Total queries considered: ", total_queries
    print "# Queries -- redirected: ", closest_to_proxy_queries[2], " open: ", closest_to_proxy_queries[0], " SDX: ", closest_to_proxy_queries[1]
    
    

def simulate_sdx():
    #get_ixp2proxy()
    #filter_ixp2proxy()
    #update_pfx2ixp()
    #get_ixp2proxy_nearest()
    #get_pfx2proxy_nearest()
    #process_pfx2proxy_nearest()
    #plot_pfx2proxy()
    #get_pfx2requests()
    #get_pfx2gfe()
    #get_pfx2ispfe()
    #get_pfx2proxy_default()
    #merge_edgecast_allprefixes()
    #compare_edgecast_allprefixes()
    #pfx2proxy_default = json.load(open('pfx2proxy_default.dat','r'))
    #print len(pfx2proxy_default.keys())
    #get_pfx2proxy_redirected()
    #get_pfx2proxy_triplet()
    analyze_pfx2proxy_triplet()


if __name__ == '__main__':
    simulate_sdx()

