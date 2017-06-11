#!/usr/bin/python

from subprocess import check_output
from datetime import datetime
import numpy
import os
import sys

class ExperimentUtil:

  def __init__(self):
    self.prev_net_diffdata = tuple()
    self.prev_cpu_total = list()
    self.prev_cpu_idle = list()
    self.prev_io_rd = list()
    self.prev_io_wr = list()
    # add current script folder to path
    sys.path.append(os.path.dirname(os.path.realpath(__file__)))

  @staticmethod
  def efficient_read(lines):
    """ just read lines very efficiently """
    prevnl = -1
    while True:
      nextnl = lines.find('\n', prevnl+1)
      if nextnl < 0:
        break
      yield lines[prevnl+1:nextnl]
      prevnl = nextnl


  # this function gets lines from cpu_measure.sh, which gets number of idle time
  # and total time of the cpu for each second, and calculates the cpu utilization
  # for the last second by calculating the difference from previous second
  # if gets cpu line parses it only
  # if get all cpus file parses them all
  def parse_cpu(self, lines):
    total = list()
    idle = list()
    for line in ExperimentUtil.efficient_read(lines):
      spl = line.split(":")
      #cpuid = int(spl[0][3:])
      spl = spl[1].split(",")
      total.append(int(spl[0]))
      idle.append(int(spl[1]))
      #print cpuid
    prevtotal = self.prev_cpu_total[:]
    previdle = self.prev_cpu_idle[:]
    self.prev_cpu_total = total
    self.prev_cpu_idle = idle
    if len(prevtotal) > 0:
      # calculates percentage for all cpus if possible
      return map(ExperimentUtil.calc_percentage, total, idle, prevtotal, previdle)
    else:
      return []

  def parse_io(self, iostat):
    for line in iostat.splitlines():
      line = line.split()
      if line[2] == "sda4":
        brd1 = str(int(line[5])*512)
        bwr1 = str(int(line[9])*512)
        #print bwr1
      elif line[2] == "sdb1":
        brd2 = str(int(line[5])*512)
        bwr2 = str(int(line[9])*512)
        #print bwr2
    prev_rd = self.prev_io_rd[:]
    prev_wr = self.prev_io_wr[:]
    self.prev_io_rd = [brd1, brd2]
    self.prev_io_wr = [bwr1, bwr2]
    if len(prev_rd) > 0:
      return [str(int(brd1) - int(prev_rd[0])), str(int(bwr1) - int(prev_wr[0]))
                , str(int(brd2) - int(prev_rd[1])), str(int(bwr2) - int(prev_wr[1]))]
    else:
      return []

  # given total, idle times in 2 time points, calculates the cpu utilization
  # of a single cpu between these 2 points.
  @staticmethod
  def calc_percentage(total, idle, prev_total, prev_idle):
    diff_total = total - prev_total
    diff_idle = idle - prev_idle
    return 0 if diff_total == 0 else 100*(diff_total - diff_idle)/float(diff_total)


  def handle_iostat(self, data):
    #iostat = check_output("iostat -dx device sda4 sdb1",
    #        shell=True)
    #iostat = filter(str.strip, iostat.splitlines())
    #iostat = map(str.split,iostat)
    #iostat = [[x[0]] + map(float, x[5:7]) for x in iostat[2:4]]
    iostat = check_output("sudo cat /proc/diskstats", shell=True)
    data['io'] = dict()
    iolist = self.parse_io(iostat)
    if len(iolist) > 0:
      data['io']['sda4'] = [iolist[0], iolist[1]]
      data['io']['sdb1'] = [iolist[2], iolist[3]]
    #for entry in iostat:
    #  data['io'][entry[0]] = entry[1:]

 
  def handle_cpustat(self, data):
    # add "all" argument to get data of all cpus
    os.chdir("/sraid/server")
    cputot = check_output("sudo ./cpu_measure.sh", shell=True)
    # cpu_desc.write(strtimenow + "$" + ", ".join(map(str,parse_cpu(cputot)))
    # + "\n")
    utilization = self.parse_cpu(cputot)
    if len(utilization) > 0:
      data["cpu"] = float(utilization[0])


  def handle_netstat(self, net_inst, data):
    # use /proc/net/dev, lo and eno1 are the used interfaces
    net_inst.update()
    diffdata = (int(net_inst['eno1']['receive']['bytes']),
            int(net_inst['eno1']['transmit']['bytes']),
            int(net_inst['lo']['receive']['bytes']),
            int(net_inst['lo']['transmit']['bytes']))
    if len(self.prev_net_diffdata) > 0:
      data["net"] = tuple(numpy.subtract(diffdata, self.prev_net_diffdata))
    self.prev_net_diffdata = diffdata

  @staticmethod
  def json_default(obj):
    """ JSON serializer for objects not serializable by default json code """
    if isinstance(obj, datetime):
      serial = obj.isoformat()
      return serial
    raise TypeError("Type not serializable")

