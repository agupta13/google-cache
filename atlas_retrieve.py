#!/usr/bin/python
import json
import os
import sys
import time
import requests

class Retrieve(object):

    URL = 'https://atlas.ripe.net/api/v1/measurement'
    
    def __init__(self, measurement_id, key=None, start=None, stop=None):
        self.measurement_id = measurement_id
        self.key = key
        self.start = start
        self.stop = stop

    def check_status(self):

        status_list = list()
        headers =  {'accept': 'application/json'}
        
        req_url = '%s/%s/%s' % (Retrieve.URL, self.measurement_id, '?fields=status')
        if self.key:
            req_url += '&key=%s' % self.key

        response = requests.get(req_url, headers=headers)
        response_str = response.text

        results = json.loads(response_str)
        status = results['status']['name']

        return status

    def fetch_results(self):
    
        headers =  {'accept': 'application/json'}

        req_url = '%s/%s/result/?' % (Retrieve.URL, self.measurement_id) 
        if self.start and self.stop:
            req_url += '&start=%d&stop=%d' % (self.start, self.stop)
        if self.key:
            req_url += '&key=%s' % self.key

        response = requests.get(req_url, headers=headers)
        response_str = response.text
            
        results = json.loads(response_str)

        return results

    def fetch_traceroute_results(self):
        #offer simplified result
        fetched_result = self.fetch_results()

        processed_results = []
        for traceroute in fetched_result:
            hop_list = []
            
            target = traceroute['dst_name']
            probe_id = traceroute['prb_id']
            hop_data_list = traceroute['result']
            
            #hop_data_list = data[0]['result']
            for hop_data in hop_data_list:
                hop_num = hop_data['hop']

                #hop = hop_data['result'][0]
                hop_found = False
                for hop in hop_data['result']: #usually 3 results for each hop
                    if 'from' in hop: #if this hop had a response
                        host = hop['from']
                        #rtt can sometimes be missing if there was a host 
                        #unreachable error
                        rtt = hop.get('rtt', -1.0)
                        ttl = hop.get('ttl', -1.0)
                        hop_list.append((hop_num, (host, rtt, ttl)))
                        hop_found = True
                        break
                
                #if we didn't find a response for this hop then 
                #fill in with anonymous router
                if not hop_found:
                    hop_list.append((hop_num, ('* * *', 0, 0)))

            hop_list.sort()
            hop_list = [x[1] for x in hop_list]
            
            result = {'status': 'finished', 'target': target, 'probe_id': probe_id, 'hops': hop_list}
            processed_results.append(result)

        return processed_results
            
    def fetch_ping_results(self):

        results = self.fetch_results()

        for (m_id, result) in results:
                
            probeid = result["prb_id"]
            target = result["dst_addr"]
            rtts = []

            for measurement in result["result"]:
                if measurement.has_key("rtt"):
                    rtt = measurement["rtt"]
                    rtts.append(rtt)
                elif measurement.has_key("error"):
                    num_error += 1
                else:
                    sys.stderr.write("measurement: "+m_id+" result has no field rtt and not field error\n")
            
            #TODO finish this

"""
if __name__ == '__main__':

    authfile = "%s/.atlas/auth" % os.environ['HOME']

    if not os.path.exists(authfile):
        raise CredentialsNotFound(authfile)

    auth = open(authfile)
    key = auth.readline()[:-1]
    auth.close()

    if len(sys.argv) != 2:
        sys.stderr.write("Usage: <measurement_id_file>\n")
        sys.exit(1)

    measurement_file = sys.argv[1]

    measurement_ids = set()

    f = open(measurement_file)
    for line in f:
        id = line.strip()
        measurement_ids.add(id)
    f.close()

    fetch_results(measurement_ids)
"""
