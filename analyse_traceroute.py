import sys
import hmvp
import json
import requests
import asrel
import ip2as as IP2AS

URL = 'https://atlas.ripe.net/api/v1/probe/?limit=10000&format=txt'
TRfile = 'outputTraceroute.txt'
probeData = 'atlas-probes.json'
asMapFile = 'asMap.txt'
asRelationFile = 'asrelOut.txt'
ixpInfoFile = 'ixpInfo.txt'


peers = {}
providercust_dict = {}
custprovider_dict = {}


def getProbe(id2info,probe_id):
    
    tmp = id2info[probe_id]
    if str(tmp[0])=='None':
        return tmp[1],tmp[3]
    return tmp[0],tmp[2]
    

def initProbeData():
    response = requests.get(URL) #make request        
    json_response = json.loads(response.text)
    
    if 'error' in json_response:
        err_msg = 'Error: %s' % json_response['error']        
        raise Exception(err_msg)
    probes = json_response['objects']
    id2info={}
    for probe in probes:
        id2info[int(probe['id'])] = (probe['asn_v4'],probe['asn_v6'],probe['prefix_v4'],probe['prefix_v6'])
    
    print "probe data init completed"
    return id2info


def load_relationship_data(providercustomer_file, peering_file):

    f = open(providercustomer_file)
    for line in f:
        line = line.strip()
        chunks = line.split()
        provider = chunks[0]
        #customers = chunks[1:]
        #include provider in customer cone
        providercust_dict[provider] = set(chunks) #customers

        customers = chunks[1:]
        for customer in customers:
            try:
                custprovider_dict[customer].add(provider)
            except KeyError:
                custprovider_dict[customer] = set(provider)
    f.close()

    f = open(peering_file)
    for line in f:
        line = line.strip()
        if line.startswith('#'):
            continue

        chunks = line.split('|')
        p1 = chunks[0]
        p2 = chunks[1]
        peering = chunks[2]
        
        if peering == '0':
            try:
                peers[p1].add(p2)
            except KeyError:
                peers[p1] = set(p2)

            try:
                peers[p2].add(p1)
            except KeyError:
                peers[p2] = set(p1)
    f.close()            


def cleanAS(asn):
    
    if '{' in asn:
        asn = asn.split('{')[1].split('}')[0]
    
    if '_' in asn:
        tmp = asn.split('_')
        asn = tmp[0]
        if float(tmp[1])>float(tmp[0]):
            asn = tmp[1]
    
    
    
    return asn


def relationship(as1, as2):
    """
    as1 is a X of as2
    """
    ispeer = False if as1 not in peers else as2 in peers[as1]
    #is as2 a customer of as1
    iscustomer = False if as2 not in providercust_dict else as1 in providercust_dict[as2]
    #is as2 a provider of as2
    isprovider = False if as2 not in custprovider_dict else as1 in custprovider_dict[as2]

    if ispeer:
        return 'peer'
    elif iscustomer:
        return 'customer'
    elif isprovider:
        return 'provider'
    else:
        return 'missing'       


def getASN(id2info):
    mapper = hmvp.ip2as.Mapper()
    fout=open(asMapFile,'w')
    with open(TRfile, 'r') as f:
        for line in f:
            tmp = line.split('\n')[0].split(',')
            hop_ips = tmp[2:]
            as_trace = mapper.mapTrace(hop_ips)
            as_out = ','.join(as_trace)
            
            probe_id=int(tmp[0])            
            probeASN,probeIP = getProbe(id2info,probe_id)
            line = ''
            if str(probeASN) != as_trace[0]:
                line = str(probeASN)+','+as_out
                print line,probe_id
                line += '\n'
            else:
                line = as_out
                print line, probe_id
                line += '\n'
            fout.write(line)
    
    fout.close()

    

            
def analyseASRelation():
    load_relationship_data('20131101.ppdc-ases.txt', '20131101.as-rel.txt')
    fout=open(asRelationFile,'w')
    with open(asMapFile, 'r') as f:
        for line in f:
            nline=''
            aspath = line.split('\n')[0].split(',')
            as1=aspath[0]
            print "line: ",line
            for asn in aspath[1:]:
                as2 = asn
                as1,as2 = cleanAS(as1),cleanAS(as2)  
                              
                r = relationship(as1, as2)
                if nline == '':
                    nline += as1+'--('+r+')->'+as2
                else:
                    nline += '--('+r+')->'+as2
                print as1,as2
                print nline
                as1 = as2
                
            nline += '\n'
            fout.write(nline)
            

def getASPaths(mapper, hops,probe_id):
    
    as_trace = mapper.mapTrace(hops)
    probeASN,probeIP = getProbe(id2info,probe_id)
    print hops, as_trace
    if str(probeASN) != as_trace[0]:
        as_trace = [str(probeASN)]+as_trace
    
    as_out = ' '.join(as_trace)
    return as_out, as_trace
    

def getASrel(aspaths):
    as_pairs = asrel.pairs(aspaths)
    print as_pairs
    rel_list = []
    for (as1, as2) in as_pairs:
        r = relationship(as1, as2)     
        rel_list.append(r)
    rel_str = ' '.join(rel_list)
    return rel_str


def getRelationData_tabFormat():
    #mapper = hmvp.ip2as.Mapper()
    mapper = ip2as.Mapper()
    load_relationship_data('20131101.ppdc-ases.txt', '20131101.as-rel.txt')
    fout=open(asRelationFile,'w')
    with open(TRfile, 'r') as f:
        for line in f:
            nline = ''
            tmp = line.split('\n')[0].split(',')
            hop_ips = tmp[2:]
            probe_id=int(tmp[0])
            aspaths,as_trace = getASPaths(mapper, hop_ips, probe_id)
            #print as_trace
            asrelations = getASrel(as_trace)
            #print asrelations
            nline += tmp[0]+'|'+tmp[1]+'|'+aspaths+'|'+asrelations+'\n'
            #print nline
            fout.write(nline)
    fout.close()
            
            
    
def getIXPInfo():
    mapper = IP2AS.Mapper() 
    load_relationship_data('20131101.ppdc-ases.txt', '20131101.as-rel.txt')
    fout=open(ixpInfoFile,'w')
    with open(TRfile, 'r') as f:
        for line in f:
            nline = ''
            tmp = line.split('\n')[0].split(',')
            hop_ips = tmp[2:]
            probe_id=int(tmp[0])
            as_trace_ixp = mapper.mapTraceIXP(hop_ips)
            asPathIXP = ' '.join(as_trace_ixp)
            aspaths,as_trace = getASPaths(mapper, hop_ips, probe_id)
            print as_trace
            asrelations = getASrel(as_trace)
            #print asrelations
            nline += tmp[0]+'|'+tmp[1]+'|'+aspaths+'|'+asrelations+'|'+asPathIXP+'\n'
            
            fout.write(nline)
             
    fout.close()


if __name__=='__main__':
    id2info = initProbeData()
    #getRelationData_tabFormat()
    getIXPInfo()
    #getASN(id2info)
    #analyseASRelation()
    
            
            
    