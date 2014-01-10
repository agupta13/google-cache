#!/usr/bin/python
import jsonrpclib
import os
import subprocess
import traceroute_service
import time

"""
Integration test for the traceroute service
"""

def assertEquals(expected, actual):
    
    try:
        assert(expected == actual)
    except AssertionError:
        print('Assertion failed. Expected %s but got %s' % (expected, actual))

def fetch_single_success(server):
    measurement_id = 1404301
    
    status = server.status(measurement_id)
    assertEquals('finished', status)
    
    results = server.results(measurement_id)
    
    assertEquals(True, isinstance(results, type([]))) #assert that results are a list
    assertEquals(1, len(results)) #assert that this one has only one entry

    assertEquals(True, isinstance(results[0], type({})))

def fetch_multiple_success(server):
    measurement_id = 1404315
    
    status = server.status(measurement_id)
    assertEquals('finished', status)
    
    results = server.results(measurement_id)
    
    assertEquals(True, isinstance(results, type([]))) #assert that results are a list
    assertEquals(2, len(results)) #assert that this one has only one entry

    assertEquals(True, isinstance(results[0], type({})))
    assertEquals(True, isinstance(results[1], type({})))

def fetch_single_failure(server):
    
    measurement_id = 1402638 #atlas status is 'Failed'
    status = server.status(measurement_id)
    assertEquals('failed', status)

    measurement_id = 1402412 #atlas status if 'No suitable probes'
    status = server.status(measurement_id)
    assertEquals('failed', status)

def fetch_ases(server):
    
    ases = server.ases()
    assertEquals(True, len(ases) > 0)
    assertEquals(True, len(ases) < 10000)
    assertEquals(True, isinstance(ases, type([]))) #should return a list
    assertEquals(True, isinstance(ases[0], type(1)))

def fetch_active(server):
    
    probe_list = server.active()
    assertEquals(True, len(probe_list) > 0)
    assertEquals(True, len(probe_list) < 10000)
    assertEquals(True, isinstance(probe_list, type([])))
    assertEquals(True, isinstance(probe_list[0], type(1)))

    ases = server.ases()
    one_as = ases[0]
    
    as_probe_list = server.active(one_as)
    assertEquals(True, len(as_probe_list) > 0)
    assertEquals(True, isinstance(as_probe_list, type([])))
    assertEquals(True, isinstance(as_probe_list[0], type(1)))

def submit_failure(server):
    
    measurement_id = server.submit([], '8.8.8.8')
    assertEquals(True, measurement_id == -1)

if __name__ == '__main__':
    
    key_file = os.path.expanduser('~')+os.sep+'.atlas/auth'
    f = open(key_file)
    key = f.read().strip()
    f.close()
    
    try:

        service = traceroute_service.TracerouteService(8080, key)
        print('Checking for active probes')
        service.check_active_probes()
        print('Done check for active probes')
        service = None

        print('Starting service')
        subprocess.Popen(['./traceroute_service.py', '8080', key])
        print('Service started')
        time.sleep(3)

        server = jsonrpclib.Server('http://localhost:8080')
    
        fetch_single_success(server)
        fetch_multiple_success(server)
        fetch_single_failure(server)
        fetch_ases(server)
        fetch_active(server)
        submit_failure(server)

    finally:
        print('Killing service')
        subprocess.Popen(['pkill', '-f', 'traceroute_service.py'])
        print('Service stopped')
    