

import subprocess
import hmvp
import traceback
import tempfile
import jsonrpclib


from atlas_traceroute import *
from traceroute_service import *
from atlas_retrieve import *
from fetch_active import *


latencyData = 'http-2014-04-15.dat'
targetList = 'probe_targetList.txt'
midsList = 'measuremendIDs.txt'
TROut = 'outputTraceroute.txt'
latencyThreshold=-1000

def kill_service():
    print('Killing service')
    subprocess.Popen(['pkill', '-f', 'traceroute_service.py'])
    print('Service stopped')
    

def get_target2probe():
    target_2_probe={}
    with open(targetList, 'r') as f:
        for line in f:
            tmp = line.split('\n')[0].split(' ')
            if tmp[1] not in target_2_probe:
                target_2_probe[tmp[1]] = []
            target_2_probe[tmp[1]].append(tmp[0]) 
    
    return target_2_probe

def filterData(thresh=250,outmap=[0,1]):
    fin=open(targetList,'w')
    print thresh
    with open(latencyData, 'r') as f:
        for line in f:
            tmp = line.split('\n')[0].split(' ')
            print tmp
            if float(tmp[5])>thresh:  
                #print tmp              
                nline=' '.join(tmp[ind] for ind in outmap)
                fin.write(nline+'\n')
                #print nline


def initTRServer():
    # Get the required Atlas key 
    key_file = os.path.expanduser('~')+os.sep+'.atlas/auth'
    with open(key_file) as f:
        key = f.read().strip()

    # Update the auth file
    auth_file = tempfile.gettempdir()+os.sep+'service_auth'
    with open(auth_file, 'w') as fauth:
        fauth.write('test:test\n')
    return key,auth_file


def startTR_Server(key,auth_file):
    
    try:
        
        service = TracerouteService(8080, key, auth_file)
        print('Checking for active probes')
        service.check_active_probes()
        print('Done check for active probes')
        service = None

        print('Starting service')
        subprocess.Popen(['./traceroute_service.py', '8080', key, auth_file])
        print('Service started')
        time.sleep(3)

        server = jsonrpclib.Server('http://test:test@localhost:8080')
        
        return server
    
    except:
        print "error starting the Traceroute Service"
        kill_service()        


def googleDefault_ASpaths(datainit=False,runTR=False):
    
    """ Parse the initial file to create the probe-target list """   
    if datainit:
        thresh=latencyThreshold
        # focusing on routes to default Google server
        outmap=[0,1]
        filterData(thresh,outmap)
    
    """ Run the traceroute measurement over Atlas nodes"""
    if runTR:
        # Start the traceroute service
        key,auth_file=initTRServer()
        server=startTR_Server(key,auth_file)
        
        print('TR Server Started')
                
        # submit the traceroute request
        target_2_probe = get_target2probe()
        measurementMap={}
        midList=[]
        for k,v in target_2_probe.iteritems():
            try:                
                mid = server.submit(v,k,key)
                midList.append(mid)
            except:
                print "error submitting request for: ",k,v
                kill_service()                
            
            # map measurement id to source,dst pair
            measurementMap[mid]= (k,v)
        print "Submitted all measurement requests", midList
        
        # consider only positive mids
        midList=filter(lambda x: x>0,midList)
        time.sleep(5)
        
        # check for status
        mid2status={}
        midsNotDone=midList
        while True:  
            print midList
            tmp_mids=[]       
            for mid in midsNotDone:
                try:
                    mid2status[mid] = server.status(mid)
                    if mid2status[mid] in ['processing','unfinished']:
                        tmp_mids.append(mid)
                except:
                    print "error checking status for ",mid
                    kill_service() 
                    
            midsNotDone=[]
            for mid in tmp_mids:
                midsNotDone.append(mid)
            
            tmp=mid2status.values()
            print tmp
            time.sleep(5)
            tmp = filter(lambda x: x in ['processing','unfinished'],tmp)
            if len(tmp)==0:
                break
        
        # filter unfinished measurement
        print mid2status
        midList = filter(lambda x: mid2status[x] not in ['failed','unfinished',
                                                         'forced to stop'],midList)
        
                
        
        try:
            # collect the results
            mid2results={}
            fout = open(TROut,'w')
            for mid in midList:
                try:
                    result = server.results(mid)
                    result=result[0]
                    mid2results[mid] = result
                    print result
                    if result['status']=='finished':
                        line = ''+result['probe_id']+','+result['target']+','+measurementMap[mid][1]
                    for hop in result['hops']:
                        line+=','+hop[0]
                    line+='\n'
                    fout.write(line)  
                except:
                    print "error in parsing result for ",mid
                 
            
            # ASPATH 
            
        except:
            print "error collecting traceroute results"
            kill_service() 
            
        finally:
            print "Completed the experiment"
            kill_service()
                    
        
        # collect the results
        
        # retrieve the traceroute results
        
    
        
        
    

if __name__=='__main__':
    
    # run the experiment to analyze AS paths to default cache nodes
    googleDefault_ASpaths(datainit=False,runTR=True)   
     
    
    
    