#!/usr/bin/python
import os
import sys
import hmvp
import traceback

latencyData='http-2014-04-15.dat'
targetList='probe_targetList.txt'
midsList='measuremendIDs.txt'
latencyThreshold=1000


def filterData(thresh=250,outmap=[0,1]):
    fin=open(targetList,'w')
    with open(latencyData, 'r') as f:
        for line in f:
            tmp = line.split(' ')
            if float(tmp[5])>thresh:                
                nline=' '.join(tmp[ind] for ind in outmap) 
                fin.write(nline+'\n')
                print nline

def googleDefault_ASpaths(datainit=False,runTR=False):
    
    """ Parse the initial file to create the probe-target list """   
    if datainit:
        thresh=latencyThreshold
        # focusing on routes to default Google server
        outmap=[0,1]
        filterData(thresh,outmap)
    
    """ Run the traceroute measurement over Atlas nodes"""
    if runTR:
        
        
    

if __name__=='__main__':
    
    # run the experiment to analyze AS paths to default cache nodes
    googleDefault_ASpaths(datainit=True,runTR=True)   
     
    
    
    