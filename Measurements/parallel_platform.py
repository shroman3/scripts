#!/usr/bin/python
import os
import sys
from subprocess import check_output, Popen, STDOUT, PIPE, CalledProcessError
from datetime import datetime #, timedelta
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

def call(cmd_list, prefix='', output=True):
    proc = Popen(cmd_list, stdout=PIPE, stderr=STDOUT)
    # Poll process for new output until finished
    while True:
        nextline = proc.stdout.readline()
        if nextline == '' and proc.poll() is not None:
            break
        if output:
            sys.stdout.write(prefix + nextline)
            sys.stdout.flush()

    output = proc.communicate()[0]
    exitCode = proc.returncode

    if (exitCode == 0):
        return output
    else:
        raise CalledProcessError(exitCode, cmd_list)

"""
This class initiates a single experiment, outputting the results from
self.servers to files $servername$.txt in the current dir of the script.
Notice that code type (zz/rs) will be specified in outer script since it needs
reinstallation of ceph.
"""
class ParallelPlatform:

    def __init__(self):
        # define dirs
        self.scdir = "/sraid"
        self.clientdir = self.scdir + "/client"
        self.serverdir = self.scdir + "/server"
        self.servers = {}
        self.experiments = []
        os.chdir(self.scdir)
        # choose servers, must be ordered
        self.parse_servers()
        if (len(sys.argv) > 3) or (len(sys.argv) < 2):
            print "Please call the parallel platform in the following manner:"
            print "./parallel_platform.py experiment_filename"
            exit(0)
        # parse arguments:
        self.parse_arguments()
        self.parse_experiments()

    def parse_experiments(self):
        with open(self.experiment, "r") as f:
            for line in f.readlines():
                line = line.strip()
                self.experiments.append(line)
                print(line)
                
    def parse_servers(self):
        e = xml.etree.ElementTree.parse('/sraid/client/config.xml').getroot()
        for conn in e.findall('connections'):
            for atype in conn.findall('server'):
                server = atype.get('host')
                self.servers[server] = self.servers.get(server, 0) + 1

    def parse_arguments(self):
        self.experiment = sys.argv[1]
        self.delete_data = False
        if (len(sys.argv) == 3):
            self.delete_data = (sys.argv[2].lower() != 'false')

    def conduct_experiment(self):
        os.chdir(self.clientdir)
        try:
            self.experiment_start = []
            # declare experiment has begun
            # self.experiment could be encode or encode2
            for exp in self.experiments:
                experiment_start = datetime.utcnow()
                print exp + " experiment started at:",
                print experiment_start
                self.experiment_start.append(experiment_start)
                p_ex = call(["java -jar Xmx1g Xms1g client.jar", exp])
                sleep(3);
        except Exception, e:
            print e

#     def fail_experiment(self):
#         # in case the size of the cluster is num_osds
#         # reweight all nodes to 1:
#         self.prepare_nodes()
#         
#         if self.analyze == "yes":
#             self.analyze_pgs(False)
#         # unmount all partitions and remount to remove cache
#         self.parallel_unmount()
#         
#         # notice that in large fail at the second we fail the OSD
#         # PGs will be remapped and recovery initiates.
#         self.prepare_failure()
# 
#         # store recovered objects before crush has updated
#         # self.store_recovered_objs(True)
# 
#         # declare experiment has begun
#         self.experiment_start = datetime.utcnow()
#         print "started experiment at " + str(self.experiment_start)
#         # print check_output("sudo ceph osd crush remove osd."
#         #        + self.experiment[-1], shell=True)
#         
#         # print check_output("sudo ceph osd unset noout", shell=True)
# 
#         # store recovered objects after crush function has changed
#         # self.store_recovered_objs(True)
# 
#         self.poll_until_health_ok() 
#         # sleeptime = (timedelta(seconds=1800)-(datetime.utcnow() \
#         #            - self.experiment_start)).total_seconds()
#         # print ("SLEEPTIME: " + str(sleeptime))
#         # if sleeptime > 0:
#         #    sleep(sleeptime)

    def analyze_pgs(self, after_experiment):
        """ Creates analyze_file(.txt) and analyze_file_v(.txt) with details
        about the PGs in the current experiment, which can later be analyzed
        """
        os.chdir(self.expdir)
        after_str = "_end" if after_experiment else ""
        analyze_file = "_".join([str(self.obj_size >> 20), str(self.k),
                str(self.r), str(self.v), str(self.s), str(self.perm),
                str((self.obj_size * self.cobj) >> 30)]) + after_str + ".txt"
        # acting_pgs = check_output("sudo ceph pg dump pgs | awk '{print $2 $16}'"
        #         + " | tail -n+2", shell=True)
        acting_pgs = check_output("sudo ceph pg dump pgs | cut -f2,16 | "
                + "tail -n+2", shell=True)
        f = open("pg_analyzer/" + analyze_file, "w")
        f.write(acting_pgs)
        f.close()
        """num_osds = self.osd_per_node * len(self.servers)
        
        print("Starting pg_analyzer.py...")
        try:
            call(["pg_analyzer/pg_analyzer.py", self.pool_name, str(self.k),
                    str(self.r), str(self.cobj), str(num_osds),
                    analyze_file])
        except Exception, e:
            print(e)"""


    def parallel_unmount(self):
        print "remounting xfs partitions on all nodes"
        os.chdir(self.expdir)
        check_output("sudo ceph osd set noout", shell=True)
        sleep(20)

        try:  # should work for everyone except the down osd
            # give client access to servers list and copy parallel_unmount.sh
            # script
            check_output("parallel-scp -l shroman "
                    + "-h /homes/cephadmin/scripts/servers.txt "
                    + "/homes/cephadmin/scripts/servers.txt /tmp", shell=True)
            check_output("parallel-scp -l shroman "
                    + "-h /homes/cephadmin/scripts/servers.txt "
                    + "/homes/cephadmin/scripts/experiments/"
                    + "parallel_unmount.sh /tmp", shell=True)
            # execute parallel_unmount.sh script to unmount and remount
            # partitions on all servers at once
            check_output("parallel-ssh -h /homes/cephadmin/scripts/servers.txt"
                    + " -l shroman 'sudo chmod +x /tmp/parallel_unmount.sh; "
                    + "/tmp/parallel_unmount.sh'"
                    , shell=True)
            # remove added files
            check_output("parallel-ssh -h /homes/cephadmin/scripts/servers.txt"
                    + " -l shroman "
                    + "'sudo rm /tmp/servers.txt /tmp/parallel_unmount.sh'"
                    , shell=True)
        except Exception, e:
            print e
        sleep(20)
        check_output("sudo ceph osd unset noout", shell=True)
        self.poll_until_health_ok()


#     """ This function crashes 1 osd in case of a small cluster failure
#     so that we can retrieve it. """
#     def prepare_failure(self):
#         # experiment named 'smallfail%' where % is the OSD to fail
#         failed_osd = int(self.experiment[-1])
#         (serv, part) = self.get_server_partition(failed_osd)
#         print "server: " + serv + " and partition: " + part
#         os.chdir(self.scdir);
#         # old way of reinserting the same OSD
#         """osd_fsid = check_output("sudo ceph-osd -i 1 --get-osd-fsid", shell=True)
#         osd_fsid = osd_fsid.replace("\n", "")
#         print osd_fsid
#         print check_output("sudo ceph osd set noout;", shell=True)
#         print check_output("sudo stop ceph-osd id=1;", shell=True)
#         print check_output("sudo umount /dev/sdb2;", shell=True)
#         print check_output("sudo mkfs.xfs -f /dev/sdb2;", shell=True)
#         # check if /var/lib/ceph/osd/ceph-1 is empty
#         print check_output("sudo rm -r /var/lib/ceph/osd/ceph-1;", shell=True)
#         print check_output("sudo mkdir /var/lib/ceph/osd/ceph-1;", shell=True)
#         print check_output("sudo mount /dev/sdb2 /var/lib/ceph/osd/ceph-1;",
#                 shell=True)
#         print check_output("sudo chown -R ceph:ceph /var/lib/ceph/osd/ceph-1;",
#                 shell=True)
#         print check_output("sudo ceph-osd -d --setuser ceph -i 1 "
#                 + "--osd-data /var/lib/ceph/osd/ceph-1 --mkfs --mkkey "
#                 + "-c /homes/cephadmin/my-cluster/ceph.conf "
#                 + "--osd-uuid " + osd_fsid + ";", shell=True)
#         print check_output("sudo ceph auth del osd.1;", shell=True)
#         print check_output("sudo ceph auth add osd.1 osd 'allow *' "
#                 + "mon 'allow rwx' -i /var/lib/ceph/osd/ceph-1/keyring;",
#                 shell=True)
#         print check_output("sudo ceph osd unset noout;", shell=True)
#         print check_output("sudo start ceph-osd id=1;", shell=True)"""
#         print check_output(["sudo", "stop", "ceph-osd", "id=1"])
#         print check_output(["sudo", "ceph", "osd", "lost", "1", "--yes-i-really-mean-it"])
#         print check_output(["sudo", "ceph", "osd", "out", "1"])
#         """try:
#             print check_output("./kill-osd.sh " 
#                     + " ".join((str(failed_osd), serv, part))
#                     , shell=True)
#         except Exception, e:
#             print e"""
 

    """ iterate over servers and partitions (ORDERED please), and get
    the i-th server and partition so that we can know the serv, part of OSD i
    """
    def get_server_partition(self, failed_osd):
        count = 0
        for s in self.servers:
            for p in self.partitions:
                if (count == failed_osd):
                    return (s, p)
                count += 1


    def store_recovered_objs(self, iterate=False):
        max_str = None
        if iterate:
            max_val = -1
            # number of times lower than max
            max_counter = 0
            # as long as we keep increasing max val, continue checking health
            while True:
                # get ceph health
                recovered_str = self.store_recovered_objs_aux()
                sleep(float(self.poll_failed_time) / 2)
                # if it mentions recovered_objects compare to max we have seen
                if recovered_str is not None:
                    recovered_objs = int(recovered_str.split("/")[0])
                    if recovered_objs > max_val:
                        max_val = recovered_objs
                        max_str = recovered_str
                        max_counter = 0
                    else:
                        # count the time we're less than max
                        max_counter += 1
                    
                    if max_counter == 5:
                        break;
        # if not iterate just get the str and output it
        else:
            while max_str == None:
                max_str = self.store_recovered_objs_aux()
                sleep(float(self.poll_failed_time) / 2)

        confname = "_".join(map(str, [self.k, self.r, self.v, self.s, self.perm]))
        f = open("pg_analyzer/recovered_objs.txt", 'a')
        f.write(confname + ": " + max_str + "\n")
        f.close()

    def store_recovered_objs_aux(self):
        os.chdir(self.expdir)
        health_str = check_output(["sudo", "ceph", "health"])
        print("storing recovered objs: " + health_str)
        recovered_str = re.match(self.rec_obj_re, health_str)
        # regexp succeeded
        if recovered_str is not None:
            recovered_str = recovered_str.group(1)
        return recovered_str
                        

    def recover_osd(self):
        failed_osd = int(self.experiment[-1])
        (serv, part) = self.get_server_partition(failed_osd)
        """check_output("ssh shroman@" + serv + " 'sudo mkdir "
                + "/var/lib/ceph/osd/ceph-" + str(failed_osd) + "'", shell=True)
        check_output("ssh shroman@" + serv +
                " 'sudo chown shroman:dslusers /var/lib/ceph/osd/ceph-"
                + str(failed_osd) + "'", shell=True)

        os.chdir("/homes/cephadmin/my-cluster")
        print "Preparing 'osd." + str(failed_osd) + "'..."
        call(("ceph-deploy --username shroman osd prepare " + serv 
                    + ":" + part).split(" "))
        

        print "Activating 'osd." + str(failed_osd) + "'..."
        call(("ceph-deploy --username shroman osd activate " + serv
                    + ":" + part).split(" "))
        # reweight osd to 1:
        call(["sudo", "ceph", "osd", "crush", "reweight", "osd."
                + str(failed_osd), "1"], output=False)"""
        print check_output("sudo ceph osd in 1", shell=True)
        print check_output("sudo start ceph-osd id=1", shell=True)
        # TODO: maybe start measurements before activate?

    def execute(self):
        print "executing: " + self.experiment + "\n"
        
        experiments = ""
        for arg in self.experiments :
            experiments += arg + "\n"
        print experiments

        self.experiment_get_ready()

        # start measurements...
        print "starting timer"
        timestart = datetime.utcnow()
        for serv, count in self.servers.iteritems():
            try:
                print "running stat_parser.py on " + serv
                stat_parser = Popen("sshpass -f p.txt ssh " + serv
                        + " '/sraid/server/stat_parser.py "
                        + timestart.isoformat() + " 2>&1 &'",
                        shell=True)
            except Exception, e:
                print e
       
        # conduct experiment...
        print "sleeping for 5 seconds"
        sleep(5)
        self.conduct_experiment()
            
        # kill measurements
        sleep(3)
        print "kill measurements by 'touch done'"
        check_output("parallel-ssh -h /homes/cephadmin/scripts/servers.txt "
                + "-l shroman -i 'touch /homes/shroman/exp/done'", shell=True)
      
        # let clients finish
        print "sleep for 3 seconds for clients to finish"
        sleep(3)
        
        self.copy_results_to_master()

        # parse experiment results
        self.parse_results()
        
        self.after_experiment()
        
    def experiment_get_ready(self):
        try:
            call(["rm", "/sraid/server/stats.txt"], output=False)
        except Exception, e:
            print e

        os.chdir(self.execdir)

        # clean up
        if self.delete_data :
            print "deleting data:"
            try:
                print check_output("sshpass -f p.txt parallel-ssh -A -h ../servers.txt "
                    + "'sudo -S rm -r /sraid/server/' < p.txt", shell=True)
            except Exception, e:
                print e
        else:
            print "deleting jar and stats:"
            try:
                print check_output("sshpass -f p.txt parallel-ssh -A -h ../servers.txt "
                    + "'sudo -S rm /sraid/server/stats.txt' < p.txt", shell=True)
            except Exception, e:
                print e
            try:
                print check_output("sshpass -f p.txt parallel-ssh -A -h ../servers.txt "
                    + "'sudo -S rm /sraid/server/server.jar' < p.txt", shell=True)
            except Exception, e:
                print e

        try:
            print check_output("sshpass -f p.txt parallel-scp -A -h ../servers.txt "
                + "../server/* /sraid/server < p.txt", shell=True)
        except Exception, e:
            print e

    def copy_results_to_master(self):
        print "copy stats to this folder"
        os.chdir(self.expdir)
        for serv in self.servers:
            check_output("scp shroman@" + serv + ":/homes/shroman/exp/stats.txt" + " ./"
                    + serv + ".txt", shell=True)
        # cleaning up clients
        print "clean client files"
        try:
            check_output("parallel-ssh -h /homes/cephadmin/scripts/servers.txt "
                    + "-l shroman -i 'rm -r /homes/shroman/exp 2>/dev/null'"
                    , shell=True)
        except Exception, e:
            print e


    def parse_results(self):
        print "parsing results..."
        os.chdir(self.expdir)
        output = open("results.txt", "a")
        csvwriter = csv.writer(output)
        csvwriter.writerow(['timestamp', 'inttime', 'experiment', 'code',
                'machine', 'osdnum', 'k', 'r', 'v', 's', 'perm', 'osd',
                'num_objects', 'obj_size', 'measurement', 'value'])
        
        for serv in self.servers:
            with open(serv + ".txt", "r") as f:
                data = json.load(f)
                for timerow in data:
                    # time, {io:{sdb2,sda3}, net:[rcv,trnsmt,localrcv,localtrnsmt],
                    #        cpu:%}

                    # parse time
                    t = dateutil.parser.parse(timerow[0])

                    # experiment haven't started
                    if self.experiment_start is not None \
                            and t < self.experiment_start:
                        continue

                    # didn't start measureent
                    if "net" not in timerow[1] or "cpu" not in timerow[1]:
                        continue

                    # write io measurements
                    if self.osd_per_node > 1:
                        self.write_measurement_row(csvwriter, "io_p1rB",
                                timerow[1]["io"]["sda3"][0], t, serv)
                        self.write_measurement_row(csvwriter, "io_p1wB",
                                timerow[1]["io"]["sda3"][1], t, serv)
                    self.write_measurement_row(csvwriter, "io_p" + str(self.osd_per_node) + "rB",
                            timerow[1]["io"]["sdb2"][0], t, serv)
                    self.write_measurement_row(csvwriter, "io_p" + str(self.osd_per_node) + "wB",
                            timerow[1]["io"]["sdb2"][1], t, serv)
                    # write network measurements
                    self.write_measurement_row(csvwriter, "net_rcvB",
                            timerow[1]["net"][0], t, serv)
                    self.write_measurement_row(csvwriter, "net_tsmtB",
                            timerow[1]["net"][1], t, serv)
                    self.write_measurement_row(csvwriter, "net_rcvB_lo",
                            timerow[1]["net"][2], t, serv)
                    self.write_measurement_row(csvwriter, "net_tsmtB_lo",
                            timerow[1]["net"][3], t, serv)
                    # write cpu utilization
                    self.write_measurement_row(csvwriter, "cpu",
                            timerow[1]["cpu"], t, serv)
                    

        output.close()

    """
    Gets a csvwriter object, measurement name, value, current time and server.
    It calculates OSD num. in case there are 20 OSDs we'd like to add 2 entries
    for network, while in all other cases there's just 1 entry per call, i.e.
    in io there are 2 partitions one for each OSD so each call is for a partition.
    """
    def write_measurement_row(self, csvwriter, measurement, value, time, server):
        printed_time = (time - self.experiment_start).total_seconds()

        osdnum = self.servers.index(server) * self.osd_per_node
        if int(self.osd_per_node) == 2 and measurement.startswith("io_p2"):
            osdnum += 1

        csvrow = [str(printed_time), str(int(printed_time)),
                self.experiment, self.code, server, str(osdnum), str(self.k),
                str(self.r), str(self.v), str(self.s), str(self.perm),
                str(self.osd_per_node), str(self.cobj), str(self.obj_size),
                measurement, str(value)];
        
        if self.osd_per_node == 2 and measurement.startswith("net"):
            csvrow[-1] = str(value / 2.0)
            csvrow2 = csvrow[:]
            csvrow2[5] = osdnum + 1
            csvwriter.writerow(csvrow)
            csvwriter.writerow(csvrow2)
        else:  # all other cases, 1 entry per call
            csvwriter.writerow(csvrow)

    """
    Iterate over all osds and for each one locate its server and partition,
    unmount it and remount it
    """
    """def delete_xfs_cache(self):
        num_osds = self.osd_per_node * len(self.servers)
        check_output("sudo ceph osd set noout", shell=True)
        for idx in range(num_osds):
            # unmount and remount
            (serv, partition) = self.get_server_partition(idx)
            try:
                check_output("ssh shroman@" + serv + " 'sudo stop ceph-osd id="
                        + str(idx) + "'", shell=True)
                check_output("ssh shroman@" + serv + " 'sudo umount /dev/"
                        + partition + "'", shell=True)
                check_output("ssh shroman@" + serv + " 'sudo mount -t xfs -o "
                        + "rw,noatime,inode64 /dev/" + partition
                        + " /var/lib/ceph/osd/ceph-" + str(idx) + "'", shell=True)
                check_output("ssh shroman@" + serv + " 'sudo start ceph-osd id="
                        + str(idx) + "'", shell=True)
            except Exception, e:
                print e
        check_output("sudo ceph osd unset noout", shell=True)"""

    def after_experiment(self):
        # retrieve lost osds if there are any:
        if self.experiment.startswith("largefail"):
            if self.analyze == "yes":
                 self.analyze_pgs(True)

            failed_osd = self.experiment[-1]
            # call(["/homes/cephadmin/scripts/repair.py", failed_osd, "2"])
            self.recover_osd()
            # self.poll_until_health_ok()
        elif self.experiment.endswith("encode"):
            pass
    


if __name__ == "__main__":
    p = ParallelPlatform()
    p.execute()
