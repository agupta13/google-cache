#!/usr/bin/python
import sys
import hmvp
import traceback

if __name__ == '__main__':

    if len(sys.argv) != 2:
        sys.stderr.write('Usage: atlas-tr.tab\n')
        sys.exit(1)

    tr_file = sys.argv[1]
    
    """
    providercust_dict = {}
    custprovider_dict = {}
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
                custprovider[customer].add(provider)
            except KeyError:
                custprovider[customer] = set(provider)
    f.close()

    peers = {}
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
    """

    mapper = hmvp.ip2as.Mapper()

    f = open(tr_file)
    for line in f:
        try:
            line = line.strip()
            chunks = line.split() 
            hops_str = chunks[2]        

            hop_ips = list()
            hop_chunks = hops_str.split('|')
            for hop_chunk in hop_chunks:
                hop_bits = hop_chunk.split(',')
                ip = hop_bits[0]
                hop_ips.append(ip)

            #as_trace = mapper.mapPairs(hop_ips)
            #as_trace = map(lambda x: '(%s %s)' % x, as_trace)
            as_trace = mapper.mapTrace(hop_ips)
            as_out = ' '.join(as_trace)
            print(as_out)

        except:
            traceback.print_exc(file=sys.stderr)

    f.close()
