#!/usr/bin/python
import sys
import hmvp
import traceback

peers = {}
providercust_dict = {}
custprovider_dict = {}

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

def pairs(as_path):
    p = []
    for i in range(0, len(as_path)-1):
        pair = (as_path[i], as_path[i+1])
        p.append(pair)
    return p

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

if __name__ == '__main__':

    if len(sys.argv) != 4:
        sys.stderr.write('Usage: <provider-customer> <peer-file> <aspaths>\n')
        sys.exit(1)

    providercustomer_file = sys.argv[1]
    peering_file = sys.argv[2]
    aspath_file = sys.argv[3]
    
      
    paths = list()
    f = open(aspath_file)
    for line in f:
        line = line.strip()
        path = line.split()
        paths.append(path)
    f.close()

    count = 0
    for path in paths:
        count += 1

        if len(path) == 0 or path[-1] == '15169' or path[-1] == '36040':
            continue

        as_pairs = pairs(path)
        rel_list = list()
        for (as1, as2) in as_pairs:
            r = relationship(as1, as2)     
            rel_list.append(r)
        rel_str = ' '.join(rel_list)

        print('%d %s %s %d' % (len(path), path, rel_str, count))        
