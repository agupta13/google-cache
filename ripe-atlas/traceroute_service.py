#!/usr/bin/python
from jsonrpclib.SimpleJSONRPCServer import SimpleJSONRPCServer, SimpleJSONRPCRequestHandler
from SocketServer import ForkingMixIn
from threading import Timer
import socket
import atlas_traceroute
import atlas_retrieve
import fetch_active
import urllib
import datetime
import tempfile
import os
import sys
import threading
import glob
import json
import itertools
import base64
import logging
import logging.config
import requests
import time
import traceback

ACTIVE_PROBES_URL = 'https://atlas.ripe.net/api/v1/probe/?limit=10000&format=txt'
ACTIVE_FILE = 'atlas-active-%d-%d-%d-%d-%d-%d'
MISSING_PROBE_ERR = 'Your selection of probes contains at least one probe that is unavailable'
ACTIVE_PROBE_INTERVAL = 21600 #every 6 hours

class SimpleForkingJSONRPCServer(ForkingMixIn, SimpleJSONRPCServer):
    
    def __init__(self, addr, requestHandler=SimpleJSONRPCRequestHandler,
                 logRequests=True, encoding=None, bind_and_activate=True,
                 address_family=socket.AF_INET, auth_map=None):

        self.auth_map = auth_map
        SimpleJSONRPCServer.__init__(self, addr, requestHandler, logRequests,
                                     encoding, bind_and_activate, address_family)

class SecuredHandler(SimpleJSONRPCRequestHandler):

    def __init__(self, request, client_address, server, client_digest=None):
        self.logger = logging.getLogger(__name__)
        self.auth_map = server.auth_map
        SimpleJSONRPCRequestHandler.__init__(self, request, client_address, server)
        self.client_digest = client_digest

    def do_POST(self):

        if self.auth_map != None:
            if self.headers.has_key('authorization') and self.headers['authorization'].startswith('Basic '):
                authenticationString = base64.b64decode(self.headers['authorization'].split(' ')[1])
                if authenticationString.find(':') != -1:
                    username, password = authenticationString.split(':', 1)
                    self.logger.info('Got request from %s:%s' % (username, password))

                    if self.auth_map.has_key(username) and self.verifyPassword(username, password):
                        return SimpleJSONRPCRequestHandler.do_POST(self)
                    else:
                        self.logger.error('Authentication failed for %s:%s' % (username, password))
            
            self.logger.error('Authentication failed')
            self.send_response(401)
            self.end_headers()
            return False

        return SimpleJSONRPCRequestHandler.do_POST(self)

    def verifyPassword(self, username, givenPassword):
        return self.auth_map[username] == givenPassword
    
class TracerouteService(object):
    
    def __init__(self, port, api_key, auth_map):
        self.logger = logging.getLogger(__name__)
        self.last_active_date = datetime.datetime(1, 1, 1) 
        self.probes = None
        self.port = port
        self.key = api_key
        self.lock = threading.RLock()
        self.auth_map = auth_map
        self.active_probe_interval = ACTIVE_PROBE_INTERVAL
        self.fetching_now = False

        self.sess = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3, pool_connections=5, pool_maxsize=10)
        self.sess.mount('https://', adapter)

    def submit(self, probe_list, target, user_key=None):
        try:
            self.logger.info('Got submit request for target %s with %s probes supplied key: %s' % (target, str(probe_list), str(user_key)))

            key = user_key if user_key is not None else self.key

            tr = atlas_traceroute.Traceroute(target, key, sess=self.sess)
            tr.num_probes = len(probe_list)
            tr.probe_type = 'probes'
            tr.probe_value = atlas_traceroute.setup_probe_value('probes', probe_list)

            response = tr.run()
            self.logger.info('Atlas response %s' % (str(response)))

            return_value = None
            if 'error' in response:
                error_details = response['error']
                code = error_details['code']
                message = error_details['message']
                self.logger.error('Got error: %s code: %d' % (message, code))
                #return_value = ('error', message+' code: '+str(code))
                if code == 103: #concurrent measurement limit
                    return_value = -2
                elif code == 104: #likely too many measurements running to a single target
                    if message == MISSING_PROBE_ERR:
                        #may need to fetch probes again
                        if not self.fetching_now:
                            self.logger.info('User submitted unavailable probe. Fetching new probefile') 
                            self.fetch_new_probefile()
                        else:
                            """
                            This could be bad. If another unavailable probe request has already initiated the request
                            then we need to block returning until the update has returned
                            """
                            self.logger.info('Delaying return until fetching new probe file completes')
                            while self.fetching_now:
                                time.sleep(0.5)

                        return_value = -3
                    else:
                        return_value = -4
                else:
                    return_value = -1
            elif 'measurements' in response:
                measurement_list = response['measurements']
                measurement_id = measurement_list[0]
                self.logger.info('Got back measurement id: %d' % measurement_id)
                return_value = measurement_id
            else:
                self.logger.error('Error processing response: %s' % str(response))
                return_value = -1;

            self.logger.info('submit returning %d' % return_value)
            return return_value
        except Exception, e:
            self.logger.error('Got exception for submit request for target %s with %s probes' % 
                              (target, str(probe_list)), exc_info=True)
            raise e

    def status(self, measurement_id):
        try:
            self.logger.info('Got status request for measurement_id %d' % (measurement_id))
            
            retrieve = atlas_retrieve.Retrieve(measurement_id, self.key, sess=self.sess)
            atlas_status = retrieve.check_status()
            return self.to_servicestatus(atlas_status)
        except Exception, e:
            self.logger.error('Got exception for status with measurement_id %d' % measurement_id, exc_info=True)
            raise e

    def to_servicestatus(self, atlas_status):

        self.logger.info('mapping atlas_status: %s' % atlas_status)

        convert_dict = {'Specified': 'processing',
                        'Scheduled': 'processing',
                        'Ongoing': 'unfinished',
                        'Stopped': 'finished', 
                        'Forced to stop': 'forced to stop',
                        'No suitable probes': 'failed',
                        'Failed': 'failed',
                        'Archived': 'finished',
                        'Stopped AS': 'finished',
                        'Archived AS': 'finished',
                        'Archived': 'finished'}
        try:
            return convert_dict[atlas_status]
        except KeyError:
            self.logger.error('Unable to map atlas_status: %s' % atlas_status)
            return 'unknown'

    def active(self, asn = None):
        try:
            self.logger.info('Got active request for asn: %s' % str(asn))

            if asn is None:
                #flatten list of lists. this is magick.
                return list(itertools.chain(*self.probes.values()))
            else:
                try:
                    return self.probes[asn]
                except KeyError:
                    return []       #return empty list if this asn is not found
        except Exception, e:
            self.logger.error('Got exception with active request for asn %s' % str(asn), exc_info=True)
            raise e

    def ases(self):
        try:
            self.logger.info('Got ases request')
            return self.probes.keys()
        except Exception, e:
            self.logger.error('Got exception for ases request', exc_info=True)
            raise e

    def results(self, measurement_id):
        try:
            self.logger.info('Got results request for measurement_id: %d' % measurement_id)
            retrieve = atlas_retrieve.Retrieve(measurement_id, self.key, sess=self.sess)
            results = retrieve.fetch_traceroute_results()

            for result in results:
                #convert probe_id to be a string
                result['probe_id'] = str(result['probe_id']) 

            #logger.info('measurementid: %d results: %s' % (measurement_id, str(results)))
            return results
        except Exception, e:
            self.logger.error('Got exception for results request for measurement_id: %d' % measurement_id, exc_info=True)
            raise e

    def check_active_probes(self):

        tempdir = tempfile.gettempdir()
        now = datetime.datetime.now()

        if self.probes is None:
            self.logger.info('No probes configured')
            #this should only happen when we first start up
            
            active_probe_list = glob.glob(tempdir+os.sep+'atlas-active-*')
            active_probe_list.sort()

            if len(active_probe_list) > 0:
                most_recent_file = active_probe_list[-1]
                self.logger.info('Most recent active probe file found: '+most_recent_file)

                basename = os.path.basename(most_recent_file)
                chunks = basename.split('-')

                year = int(chunks[2])
                month = int(chunks[3])
                day = int(chunks[4])
                hour = int(chunks[5])
                minute = int(chunks[6])
                second = int(chunks[7])

                most_recent_date = datetime.datetime(year, month, day, hour, minute, second)

                timediff = now - most_recent_date
                if timediff.seconds < self.active_probe_interval:
                    try:
                        self.load_probes(most_recent_file)
                        self.last_active_date = most_recent_date
                        self.logger.info('last_active_date for probe file is %s' % self.last_active_date)
                    except Exception, e:
                        self.logger.error('Failed to load %s' % most_recent_file, exc_info=True)
                        self.logger.error('Fetching new file instead')
                        self.fetch_new_probefile()
                    return
                else:
                    self.logger.info('Most recent file was out of date')
            else:
                self.logger.info('No active-probe files found')
        
        #first check that we have the latest file for today
        timediff = now - self.last_active_date
        if timediff.seconds >= self.active_probe_interval:
            self.fetch_new_probefile()
            return

    def schedule_probe_check(self):
        self.logger.info('running check for active probes')
        self.check_active_probes()
        self.logger.info('rescheduling probe check')
        Timer(self.active_probe_interval, self.schedule_probe_check).start()	

    def fetch_new_probefile(self):
        
        if self.fetching_now:
            self.logging.error('attempted to fetch new probe file in another process')
            return
        
        self.fetching_now = True
        try:
            now = datetime.datetime.now()
            self.logger.info('Started fetching new probe file at %s' % str(now))
            tempdir = tempfile.gettempdir()

            save_file_name = ACTIVE_FILE % (now.year, now.month, now.day, now.hour, now.minute, now.second)
            save_file_path = '%s%s%s' % (tempdir, os.sep, save_file_name)
            #fetch new active file
            self.logger.info('Fetching new active probe file to: '+save_file_path)
            #urllib.urlretrieve(ACTIVE_PROBES_URL, save_file_path)
            probe_list = fetch_active.fetch_probes() #fetch only active probes
            
            #write json objects to string and save file
            #probe_outstr = json.dumps(probe_list, sort_keys=True, indent=4, separators=(',', ': '))
            lines = fetch_active.json2tab(probe_list)
            probe_outstr = '\n'.join(lines)     
    
            f = open(save_file_path, 'w')
            f.write(probe_outstr)
            f.close() 

            self.logger.info('Finished fetching at %s' % str(datetime.datetime.now()))

            self.load_probes(save_file_path)
            self.logger.info('Finished loading new probe file')
            self.last_active_date = now #update latest time we fetched
            self.logger.info('last_active_date for probe file is %s' % self.last_active_date)
        finally:
            self.fetching_now = False

    def load_probes(self, filename):
       
        probes_list = fetch_active.load(filename)
        #print(probes_dict)
        active_probes = {}

        #probes_dict = all_probes['objects']
        self.logger.info('Processing '+str(len(probes_list))+' probes')
        
        for probe in probes_list:
            try:
                id = str(probe['id'])
                status = probe['status_name']
                #prefix = probe['prefix_v4']
                #country = probe['country_code']
                asn = probe['asn_v4']
                
                if status == 'Connected':
                    try:
                        active_probes[asn].append(id)
                    except KeyError:
                        active_probes[asn] = [id]
            except:
                traceback.print_exc(file=sys.stderr)
                continue

        #I'm *pretty sure* that assignments in Python are atomic
        #Otherwise, this could cause some pain
        self.probes = active_probes

        num_probes = sum(len(l) for l in self.probes.values())
        self.logger.info('Loaded: '+filename+' with '+str(num_probes)+' active probes')

    def run(self):

        self.schedule_probe_check()

        server = SimpleForkingJSONRPCServer(('', self.port), requestHandler=SecuredHandler, auth_map=self.auth_map)

        server.register_function(self.ases, 'ases')
        server.register_function(self.submit, 'submit')
        server.register_function(self.active, 'active')
        server.register_function(self.results, 'results')
        server.register_function(self.status, 'status')

        self.logger.info('Starting service on port: %d' % self.port)
        server.serve_forever()

def setup_logging(default_path='logging.json', default_level=logging.INFO, env_key='LOG_CFG'):
    """
    Setup logging configuration
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

def load_auth(auth_file):
    auth_map = {}
    
    with open(auth_file) as f:
        for line in f:
            (user, password) = line.strip().split(':')
            auth_map[user] = password
    
    return auth_map

if __name__ == '__main__':
    
    if len(sys.argv) != 4:
        sys.stderr.write('Usage: <port> <key> <auth_file>\n')
        sys.exit(1)

    port = int(sys.argv[1])
    key = sys.argv[2]
    auth_file = sys.argv[3]
    
    setup_logging()
    auth_map = load_auth(auth_file)

    service = TracerouteService(port, key, auth_map)
    service.run()
