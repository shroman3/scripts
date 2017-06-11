#!/usr/bin/python
import os
import sys
from subprocess import check_output, Popen, STDOUT, PIPE, CalledProcessError
from datetime import datetime  # , timedelta
from time import sleep
import csv
import dateutil.parser
import json
import re
import xml.etree.ElementTree
# from fileinput import filename


# list of functions:
# call(cmd_list, prefix, output) - call a shell command streaming stdout
# __init__() - mainly initialize parameters from argv
# conduct_experiment() - prepare cluster for failure if failure experiment
#   or remove/add nodes in case it was changed from last configuration and wait
#   for cluster to balance
# prepare_nodes() - remove or add nodes according to 10/20 OSD configuration
# prepare_failure() - ONLY SUPPORTS LARGEFAIL! removes node, should start
#   measurement at the moment this function finishes.
# get_server_partition() - encodes osd num to its server and partition
# recover_osd() - recovers the failed osd and waits until cluster is balanced
# poll_until_health_ok() - poll ceph health until HEALTH_OK or up to 10 errors
# execute() - does the main script, measure, conduct experiment, and parse
#   the measurements eventually.
# parse_results() - parses the output files from each server to a proper csv
#   named results.txt
# write_measurement_row() - used to save redundant copy paste in parse_results

# def call(cmd_list, prefix='', output=True):
#     proc = Popen(cmd_list, stdout=PIPE, stderr=STDOUT)
#     # Poll process for new output until finished
#     while True:
#         nextline = proc.stdout.readline()
#         if nextline == '' and proc.poll() is not None:
#             break
#         if output:
#             sys.stdout.write(prefix + nextline)
#             sys.stdout.flush()
# 
#     output = proc.communicate()[0]
#     exitCode = proc.returncode
# 
#     if (exitCode == 0):
#         return output
#     else:
#         raise CalledProcessError(exitCode, cmd_list)

def run_command(command):
    try:
        print check_output(command, shell=True)
    except Exception as e:
        print e
        
def parallelssh(command):
    try:
        print check_output("sshpass -f p.txt parallel-ssh -A -h ../servers.txt '" + command + "' < p.txt", shell=True)
    except Exception as e:
        print e

def parallelscp(copy_from, copy_to):
    try:
        print check_output("sshpass -f p.txt parallel-scp -A -h ../servers.txt "
            + copy_from + " " + copy_to + " < p.txt", shell=True)
    except Exception, e:
        print e
"""
This class initiates a single experiment, outputting the results from
self.servers to files $servername$.txt in the current dir of the script.
Notice that code type (zz/rs) will be specified in outer script since it needs
reinstallation of ceph.
"""
class ParallelPlatform:
    def __init__(self):
        # define dirs
        self.scdir = "/home/shroman/sraid"
        self.scriptsdir = self.scdir + "/scripts"
        self.resultsdir = self.scdir + "/results"

        os.chdir(self.scdir)
        
        self.servers = {}
        # choose servers, must be ordered
        if (len(sys.argv) != 2):
            print "Please call the parallel platform in the following manner:"
            print "./parallel_platform.py experiment_filename"
            exit(0)
        # parse arguments:
        self.parse_servers()
        self.parse_experiment(sys.argv[1])

    def parse_experiment(self, filename):
        os.chdir(self.scriptsdir)
        with open(filename, "r") as f:
            self.experiment = f.readline()

        self.is_write = (self.experiment.startswith('w') or self.experiment.startswith('W'))

    def parse_servers(self):
        run_command("rm servers.txt")

        e = xml.etree.ElementTree.parse('client/config.xml').getroot()
        for conn in e.findall('connections'):
            for atype in conn.findall('server'):
                server = atype.get('host')
                self.servers[server] = self.servers.get(server, 0) + 1

        with open('servers.txt', 'w') as serversfile:
            for serv in self.servers.keys():
                serversfile.write("%s\n" % serv)

    def experiment_get_ready(self):
        run_permissions_command = "chmod +x /sraid/server/*.sh /sraid/server/*.py"

        # client clean up
        run_command("sudo rm -r /sraid/server/*")  
        run_command("cp ../client/* /sraid/client/")
        run_command("cp ../server/* /sraid/server/")
        run_command(run_permissions_command)

        # servers clean up
        if self.is_write :
            print "deleting data:"
            parallelssh("sudo -S rm -r /sraid/server/*")
            parallelssh("sudo -S rm -r /sraid2/server/*")
        else:
            print "deleting jar and stats:"
            parallelssh("sudo -S rm /sraid/server/stats.txt")
            parallelssh("sudo -S rm /sraid/server/done")

        parallelscp("../server/*", "/sraid/server/")
        parallelscp("../server/server.jar", "/sraid2/server/")

        parallelssh(run_permissions_command)
        
    def start_serevrs(self):
        print "STARTING SERVERS"
        parallelssh("sraid/scripts/startserver.sh")
            
    def stop_serevrs(self):
        print "STOPING SERVERS"
        parallelssh("sraid/scripts/stopserver.sh")

    def conduct_experiment(self):
        os.chdir("/sraid/client")
        try:
            # If the experiment is write need to load the file to memory before the experiment
            if (self.is_write):
                print "Reading input file before writing it"
                with open("0", mode='rb') as file:
                    fileContent = file.read()
            self.experiment_start = datetime.utcnow()
            print self.experiment + " experiment started at:",
            print self.experiment_start
            check_output("java -jar client.jar " + self.experiment, shell=True)
        except Exception, e:
            print "FAILED"
            print e

 #     def parallel_unmount(self):
#         print "remounting xfs partitions on all nodes"
#         os.chdir(self.expdir)
#         check_output("sudo ceph osd set noout", shell=True)
#         sleep(20)
# 
#         try:  # should work for everyone except the down osd
#             # give client access to servers list and copy parallel_unmount.sh
#             # script
#             check_output("parallel-scp -l shroman "
#                     + "-h /homes/cephadmin/scripts/servers.txt "
#                     + "/homes/cephadmin/scripts/servers.txt /tmp", shell=True)
#             check_output("parallel-scp -l shroman "
#                     + "-h /homes/cephadmin/scripts/servers.txt "
#                     + "/homes/cephadmin/scripts/experiments/"
#                     + "parallel_unmount.sh /tmp", shell=True)
#             # execute parallel_unmount.sh script to unmount and remount
#             # partitions on all servers at once
#             check_output("parallel-ssh -h /homes/cephadmin/scripts/servers.txt"
#                     + " -l shroman 'sudo chmod +x /tmp/parallel_unmount.sh; "
#                     + "/tmp/parallel_unmount.sh'"
#                     , shell=True)
#             # remove added files
#             check_output("parallel-ssh -h /homes/cephadmin/scripts/servers.txt"
#                     + " -l shroman "
#                     + "'sudo rm /tmp/servers.txt /tmp/parallel_unmount.sh'"
#                     , shell=True)
#         except Exception, e:
#             print e
#         sleep(20)
#         check_output("sudo ceph osd unset noout", shell=True)
#         self.poll_until_health_ok()
        
    def kill_measurements(self):
        # kill measurements
        os.chdir(self.scriptsdir)
        
        sleep(3)
        self.stop_serevrs()
        
        print "kill measurements by 'touch done'"
        touch_done_command = "touch /sraid/server/done"
        parallelssh(touch_done_command)
        run_command(touch_done_command)
        # let clients finish
        print "sleep for 3 seconds for clienls -l ../resu    ts to finish"
        sleep(3)

    def execute(self):
        print "executing: " + self.experiment

        self.stop_serevrs()
        self.experiment_get_ready()
        self.start_serevrs()
        
        # start measurements...
        print "starting timer"
        timestart = datetime.utcnow()
#         for serv in self.servers.keys():
#             try:
#                 print "running stat_parser.py on " + serv
#                 stat_parser = Popen("sshpass -f p.txt ssh " + serv
#                         + " 'nohup /sraid/server/stat_parser.py "
#                         + timestart.isoformat() + " >/dev/null 2>&1 &'",
#                         shell=True)
#             except Exception, e:
#                 print e
        run_stats_parser_command = "nohup /sraid/server/stat_parser.py "  + timestart.isoformat() + " >/dev/null 2>&1 &"
        parallelssh(run_stats_parser_command)
        run_command(run_stats_parser_command)        
        
        # conduct experiment...
        print "sleeping for 5 seconds"
        sleep(5)
        self.conduct_experiment()

        self.kill_measurements()
        
        self.copy_results_to_master()

        # parse experiment results
        self.parse_results()
        self.parse_logs()  

        
    def copy_results_to_master(self):
        os.chdir(self.scriptsdir)
        print "copy stats to results folder"
        for serv in self.servers:
            run_command("sshpass -f p.txt scp " + serv + ":/sraid/server/stats.txt ../results/" + serv + ".stat < p.txt")
            run_command("sshpass -f p.txt scp " + serv + ":/sraid/server/logs/client_connection.log ../results/log1_" + serv + ".log < p.txt")
            run_command("sshpass -f p.txt scp " + serv + ":/sraid2/server/logs/client_connection.log ../results/log2_" + serv + ".log < p.txt")
            
        run_command("cp /sraid/client/logs/* ../results/");
        run_command("cp /sraid/server/stats.txt ../results/client.stat");

    def parse_results(self):
        print "parsing results..."
        os.chdir(self.resultsdir)
        output = open("results.txt", "a")
        csvwriter = csv.writer(output)
        csvwriter.writerow(['timestamp', 'inttime', 'experiment', 'machine', 'measurement', 'value'])
        
        files = [f for f in os.listdir('.') if (os.path.isfile(f) and f.endswith(".stat"))]
        for file in files:
            print file
            serv=file.split('.')[0]
            with open(file, "r") as f:
                data = json.load(f)
                for timerow in data:
                    # parse time
                    t = dateutil.parser.parse(timerow[0])
                    # experiment haven't started
                    if self.experiment_start is not None \
                            and t < self.experiment_start:
                        continue

                    # didn't start measureent
                    if "net" not in timerow[1] or "cpu" not in timerow[1]:
                        continue

                    self.write_measurement_row(csvwriter, "io_p1rB", timerow[1]["io"]["sda4"][0], t, serv)
                    self.write_measurement_row(csvwriter, "io_p1wB", timerow[1]["io"]["sda4"][1], t, serv)
                    self.write_measurement_row(csvwriter, "io_p2rB", timerow[1]["io"]["sdb1"][0], t, serv)
                    self.write_measurement_row(csvwriter, "io_p2wB", timerow[1]["io"]["sdb1"][1], t, serv)
                    # write network measurements
                    self.write_measurement_row(csvwriter, "net_rcvB", timerow[1]["net"][0], t, serv)
                    self.write_measurement_row(csvwriter, "net_tsmtB", timerow[1]["net"][1], t, serv)
                    self.write_measurement_row(csvwriter, "net_rcvB_lo", timerow[1]["net"][2], t, serv)
                    self.write_measurement_row(csvwriter, "net_tsmtB_lo", timerow[1]["net"][3], t, serv)
                    # write cpu utilization
                    self.write_measurement_row(csvwriter, "cpu", timerow[1]["cpu"], t, serv)
                    

        output.close()

    """
    Gets a csvwriter object, measurement name, value, current time and server.
    It calculates OSD num. in case there are 20 OSDs we'd like to add 2 entries
    for network, while in all other cases there's just 1 entry per call, i.e.
    in io there are 2 partitions one for each OSD so each call is for a partition.
    """
    def write_measurement_row(self, csvwriter, measurement, value, time, server):
        printed_time = (time - self.experiment_start).total_seconds()

#         osdnum = self.servers.index(server) * self.osd_per_node
#         if int(self.osd_per_node) == 2 and measurement.startswith("io_p2"):
#             osdnum += 1
        
        # ['timestamp', 'inttime', 'experiment', 'machine', 'measurement', 'value'])
        csvrow = [str(printed_time), str(int(printed_time)),
                  self.experiment, server, measurement, str(value)];
        
        csvwriter.writerow(csvrow)
#         if measurement.startswith("net"):
#             csvrow[-1] = str(value / 2.0)
#             csvrow2 = csvrow[:]
#             csvrow2[5] = osdnum + 1
#             csvwriter.writerow(csvrow)
#             csvwriter.writerow(csvrow2)
#         else:  # all other cases, 1 entry per call
#             csvwriter.writerow(csvrow)
    def parse_logs(self):
        print "parsing logs..."
        os.chdir(self.resultsdir)
        output = open("logs.txt", "a")
        csvwriter = csv.writer(output)
        csvwriter.writerow(['experiment','log_type', 'time', 'tag', 'size'])
        
        files = [f for f in os.listdir('.') if (os.path.isfile(f) and f.endswith(".log"))]
        for file in files:
            print file
            log_type=file.split('.')[0]
            with open(file, "r") as f:
                for line in f:
                    try:
                        split = line.split(' - ')
                        timestamp=split[0]
                        split = re.compile("\]*[\s\w,-:]*\[+").split(split[1])
                        self.write_logs_row(csvwriter, log_type, split[2], split[3], split[4])
                    except Exception, e:
                        print e
                       
        output.close()
#         print "removing logs and stats"
#         run_command("rm *.log *.stat")

    def write_logs_row(self, csvwriter, log_type, time, tag, size):
        # ['experiment', 'log_type', 'time', 'tag', 'size'])
        size = size.split(']')[0]
        csvrow = [self.experiment, log_type, time, tag] + size.split(',');
        csvrow.append(object)
        csvwriter.writerow(csvrow)

if __name__ == "__main__":
    p = ParallelPlatform()
    p.execute()
