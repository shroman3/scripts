#!/usr/bin/python

from time import sleep
from datetime import datetime, timedelta
from procnetdev import ProcNetDev
from experiment_util import ExperimentUtil
import json
import os.path
from os import chdir
import dateutil.parser # use dateutil.parser.parse(isotime) to get datetime obj
import sys
from subprocess import check_output

# main 
def main():
  chdir("/sraid/server/")
  if len(sys.argv) < 2:
    timestart = datetime.utcnow()
    # print "Provide start time..."
    # return 1
  else: 
    timestart = dateutil.parser.parse(sys.argv[1])
  #print "Execute 'touch done' for script to finish..."
  exp = ExperimentUtil()

  #print timestart.strftime("%y-%m-%d %H:%M:%S.%f")
  
  net_inst = ProcNetDev(auto_update=False)
  
  # for each timestamp we add a pair to the list:
  # (timestamp, dict("io": {}, "net": {}, "cpu": {}))
  # io dict: "sdb2": [rkB/s, wkB/s], "sda3": [rkB/s, wkB/s] (positions 5,6 in
  #   splitted list)
  # net dict: "lo": [rB/s, tB/s], "eno1": [rB/s, tB/s]
  # cpu: total %util
  gathered_data = list()
 
  elapsed = (datetime.utcnow() - timestart).seconds + 1
  #print "start elapsed: " + str(elapsed)
  #print check_output("pwd")
  while (not os.path.isfile("done")):
  #for i in range(5): # will be while xx.poll() or something
    data = dict()
    current_time = timestart + elapsed*timedelta(seconds=1)
        
    exp.handle_iostat(data)
    exp.handle_cpustat(data)
    exp.handle_netstat(net_inst, data)

    gathered_data.append((current_time, data))

    # print data
    
    # handle accurate sleep time
    future = timestart + (1+elapsed)*timedelta(seconds=1)
    timenow = datetime.utcnow()
    # print elapsed, (future-timenow).total_seconds()
    # sleep until the next second since timestart
    sleeptime = (future-timenow).total_seconds()
    #print sleeptime
    if sleeptime > 0:
        sleep(sleeptime)
    elapsed += 1
  
  #print "elapsed:", elapsed
  
  stats_desc = open("stats.txt", "w")
  json.dump(gathered_data, stats_desc, default=ExperimentUtil.json_default)
  stats_desc.close()
  
  timeend = datetime.utcnow()
  #print timeend.strftime("%y-%m-%d %H:%M:%S.%f")
  
  #proc1 = Popen("ls -l", shell=True)
  #sleep(2.0)
  #print 'proc1 = ', proc1.pid
  #Popen.kill(proc1)

if __name__ == "__main__":
  #print "Hi"
  main()
