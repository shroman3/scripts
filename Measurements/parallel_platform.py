#!/usr/bin/python
import os
import sys
from subprocess import check_output, Popen, STDOUT, PIPE, CalledProcessError
from datetime import datetime, timedelta
from time import sleep
import csv
import dateutil.parser
import json
import re
import xml.etree.ElementTree


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
        self.scdir = "/homes/sraid/scripts"
        self.expdir = self.scdir + "/experiments"
        # self.rec_obj_re = re.compile('.*recovery (\S*) objects')
        self.servers = []
        os.chdir(self.scdir)
        # choose servers, must be ordered
        self.parse_servers()
#         with open("servers.txt", "r") as f:
#             for line in f.readlines():
#                 self.servers.append(line.strip())
#         self.partitions = ["sda3", "sdb2"]
# TODO: Check what to get as arguments
        if len(sys.argv) < 6:
            print "Please call the parallel platform in the following manner:"
            print "./parallel_platform.py <experiment> <k> <r> <z> <osd>"
            exit(0)
        # parse arguments:
        self.parse_arguments()

    def parse_servers(self):
        e = xml.etree.ElementTree.parse('config.xml').getroot()
        for atype in e.findall('server'):
            server = atype.get('host')
            print(server)
            self.servers.append(server)

    def parse_arguments(self):
        self.experiment = sys.argv[1]
        if "fail" in self.experiment:
            print "Remember that we assume the pool is correctly set and " \
                + "objects were written to it"
        self.k = int(sys.argv[2])
        self.r = int(sys.argv[3])
        self.z = int(sys.argv[4])
        # self.osd_per_node will be used when parsing the measurement to an excel.
        self.osd_per_node = int(sys.argv[5])
#         self.s = int(sys.argv[5])
#         self.perm = int(sys.argv[6])
#         self.cobj = int(sys.argv[8])
#         self.obj_size = int(sys.argv[9])
#         self.code = sys.argv[10]
#         self.analyze = sys.argv[11]
#         if self.code != "zz" and self.code != "rs":
#             print "Illegal code name. Please choose zz/rs..."
#             exit(0)
        self.pool_name = "zzpool" if self.code == "zz" else "rspool"
        # self.analyze_file = None if len(sys.argv) < 12 else sys.argv[11]
        # if self.analyze_file is not None:
        #     tmp = self.analyze_file.split(".")
        #     self.end_analyze_file = tmp[0] + "_end." + tmp[1]
        self.align = 4096 # element size
        self.w = 8 # if not specified else
        serv_num = len(self.servers)
        # should be 19 in regular configuration
        self.max_num_osds = int(serv_num) * 2 - 1
        self.nodes_needed = self.k + self.r
        # if we need (k+r) more than the number of OSDs
        if self.nodes_needed > self.max_num_osds \
                or (self.osd_per_node == 1 \
                        and self.nodes_needed > self.max_num_osds / 2):
            print "num nodes needed bigger than num osds"
            # TODO: think later what to do in that case, i.e. stop measurements etc
            assert(0)
        self.pg_num = 512 # seems fine for our experiment sizes
        self.experiment_start = None
        self.poll_failed_time = 2 # num seconds to sleep between calls to ceph health on failure


    def conduct_experiment(self):
        if self.experiment.endswith("encode"):
            self.encode_experiment()
        elif self.experiment.startswith("largefail"):
            self.fail_experiment()        


    def encode_experiment(self):
        # erase + prepare pool using our erasure_commands script:
        os.chdir(self.scdir)
        # Create pool:
        try:
            p_ec = call(["sudo", "./erasure_commands.py"]
                    + list(map(str, (self.pool_name, self.k, self.r, self.v,
                    self.s, self.perm, self.w, self.align,
                    self.obj_size, self.pg_num))), "ec: ")
        except Exception, e:
            print e
        # reweight nodes properly
        self.prepare_nodes()
        # wait for cluster to rebalance
        self.poll_until_health_ok()

        os.chdir(self.expdir)
        try:
            # declare experiment has begun
            self.experiment_start = datetime.utcnow()
            print "experiment started at:",
            print self.experiment_start
            # self.experiment could be encode or encode2
            
            p_ex = call(["sudo", "./runner/bin/experiment_runner"]
                + list(map(str, (self.pool_name, self.experiment, self.k, self.r,
                            self.v, self.s, self.cobj, self.obj_size))))
        except Exception, e:
            print e

    def fail_experiment(self):
        # in case the size of the cluster is num_osds
        # reweight all nodes to 1:
        self.prepare_nodes()
        
        if self.analyze == "yes":
            self.analyze_pgs(False)
        # unmount all partitions and remount to remove cache
        self.parallel_unmount()
        
        # notice that in large fail at the second we fail the OSD
        # PGs will be remapped and recovery initiates.
        self.prepare_failure()

        # store recovered objects before crush has updated
        #self.store_recovered_objs(True)

        # declare experiment has begun
        self.experiment_start = datetime.utcnow()
        print "started experiment at " + str(self.experiment_start)
        #print check_output("sudo ceph osd crush remove osd."
        #        + self.experiment[-1], shell=True)
        
        #print check_output("sudo ceph osd unset noout", shell=True)

        # store recovered objects after crush function has changed
        #self.store_recovered_objs(True)

        self.poll_until_health_ok() 
        #sleeptime = (timedelta(seconds=1800)-(datetime.utcnow() \
        #            - self.experiment_start)).total_seconds()
        #print ("SLEEPTIME: " + str(sleeptime))
        #if sleeptime > 0:
        #    sleep(sleeptime)


    def prepare_nodes(self):
        # osds of number self.max_num_osds and above should be reweighted to 0.
        # maybe get osds using ceph osd tree parsing it...
        # for now assume that there are 8 (or 20) OSDs:

        # remove osd.i in the following manner:
        # increase nodes_needed by 1 and then reweight osd 0 to 0 (or skip its reweight)
        # now we want to treat the all_osds case:
        nodes_needed = self.max_num_osds if self.osd_per_node == 2 \
                                else self.max_num_osds // 2

        # Reweight to 1 according to the bus principle or regular?
        # Bus principle:
        nodes_order = [1,3,5,7,9,11,13,15,17,19,2,4,6,8,10,12,14,16,18,0]
        # filter away too large osds
        nodes_order = [x for x in nodes_order if x < len(self.servers)*2]
        for i in range(0, nodes_needed):
            call(["sudo", "ceph", "osd", "crush", "reweight",
                    "osd."+str(nodes_order[i]), "1"], output=False)
        # if we have 9 servers we wouldn't like to change weight of osd 18,19
        for i in range(nodes_needed, len(nodes_order)):
            call(["sudo", "ceph", "osd", "crush", "reweight",
                    "osd."+str(nodes_order[i]), "0"], output=False)

        # we caused remapping so we must wait until ceph stabilizes
        self.poll_until_health_ok()
        
        # mark nodes as down


    def analyze_pgs(self, after_experiment):
        """ Creates analyze_file(.txt) and analyze_file_v(.txt) with details
        about the PGs in the current experiment, which can later be analyzed
        """
        os.chdir(self.expdir)
        after_str = "_end" if after_experiment else ""
        analyze_file = "_".join([str(self.obj_size >> 20), str(self.k),
                str(self.r), str(self.v), str(self.s), str(self.perm),
                str((self.obj_size*self.cobj) >> 30)]) + after_str + ".txt"
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

        try: # should work for everyone except the down osd
            # give client access to servers list and copy parallel_unmount.sh
            # script
            check_output("parallel-scp -l cephuser "
                    + "-h /homes/cephadmin/scripts/servers.txt "
                    + "/homes/cephadmin/scripts/servers.txt /tmp", shell=True)
            check_output("parallel-scp -l cephuser "
                    + "-h /homes/cephadmin/scripts/servers.txt "
                    + "/homes/cephadmin/scripts/experiments/"
                    + "parallel_unmount.sh /tmp", shell=True)
            # execute parallel_unmount.sh script to unmount and remount
            # partitions on all servers at once
            check_output("parallel-ssh -h /homes/cephadmin/scripts/servers.txt"
                    + " -l cephuser 'sudo chmod +x /tmp/parallel_unmount.sh; "
                    + "/tmp/parallel_unmount.sh'"
                    , shell=True)
            # remove added files
            check_output("parallel-ssh -h /homes/cephadmin/scripts/servers.txt"
                    + " -l cephuser "
                    + "'sudo rm /tmp/servers.txt /tmp/parallel_unmount.sh'"
                    , shell=True)
        except Exception, e:
            print e
        sleep(20)
        check_output("sudo ceph osd unset noout", shell=True)
        self.poll_until_health_ok()


    """ This function crashes 1 osd in case of a small cluster failure
    so that we can retrieve it. """
    def prepare_failure(self):
        # experiment named 'smallfail%' where % is the OSD to fail
        failed_osd = int(self.experiment[-1])
        (serv, part) = self.get_server_partition(failed_osd)
        print "server: " + serv + " and partition: " + part
        os.chdir(self.scdir);
        # old way of reinserting the same OSD
        """osd_fsid = check_output("sudo ceph-osd -i 1 --get-osd-fsid", shell=True)
        osd_fsid = osd_fsid.replace("\n", "")
        print osd_fsid
        print check_output("sudo ceph osd set noout;", shell=True)
        print check_output("sudo stop ceph-osd id=1;", shell=True)
        print check_output("sudo umount /dev/sdb2;", shell=True)
        print check_output("sudo mkfs.xfs -f /dev/sdb2;", shell=True)
        # check if /var/lib/ceph/osd/ceph-1 is empty
        print check_output("sudo rm -r /var/lib/ceph/osd/ceph-1;", shell=True)
        print check_output("sudo mkdir /var/lib/ceph/osd/ceph-1;", shell=True)
        print check_output("sudo mount /dev/sdb2 /var/lib/ceph/osd/ceph-1;",
                shell=True)
        print check_output("sudo chown -R ceph:ceph /var/lib/ceph/osd/ceph-1;",
                shell=True)
        print check_output("sudo ceph-osd -d --setuser ceph -i 1 "
                + "--osd-data /var/lib/ceph/osd/ceph-1 --mkfs --mkkey "
                + "-c /homes/cephadmin/my-cluster/ceph.conf "
                + "--osd-uuid " + osd_fsid + ";", shell=True)
        print check_output("sudo ceph auth del osd.1;", shell=True)
        print check_output("sudo ceph auth add osd.1 osd 'allow *' "
                + "mon 'allow rwx' -i /var/lib/ceph/osd/ceph-1/keyring;",
                shell=True)
        print check_output("sudo ceph osd unset noout;", shell=True)
        print check_output("sudo start ceph-osd id=1;", shell=True)"""
        print check_output(["sudo", "stop", "ceph-osd", "id=1"])
        print check_output(["sudo", "ceph", "osd", "lost", "1", "--yes-i-really-mean-it"])
        print check_output(["sudo", "ceph", "osd", "out", "1"])
        """try:
            print check_output("./kill-osd.sh " 
                    + " ".join((str(failed_osd), serv, part))
                    , shell=True)
        except Exception, e:
            print e"""
 

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
                sleep(float(self.poll_failed_time)/2)
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
                sleep(float(self.poll_failed_time)/2)

        confname = "_".join(map(str,[self.k, self.r, self.v, self.s, self.perm]))
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
        """check_output("ssh cephuser@" + serv + " 'sudo mkdir "
                + "/var/lib/ceph/osd/ceph-" + str(failed_osd) + "'", shell=True)
        check_output("ssh cephuser@" + serv +
                " 'sudo chown cephuser:dslusers /var/lib/ceph/osd/ceph-"
                + str(failed_osd) + "'", shell=True)

        os.chdir("/homes/cephadmin/my-cluster")
        print "Preparing 'osd." + str(failed_osd) + "'..."
        call(("ceph-deploy --username cephuser osd prepare " + serv 
                    + ":" + part).split(" "))
        

        print "Activating 'osd." + str(failed_osd) + "'..."
        call(("ceph-deploy --username cephuser osd activate " + serv
                    + ":" + part).split(" "))
        # reweight osd to 1:
        call(["sudo", "ceph", "osd", "crush", "reweight", "osd."
                + str(failed_osd), "1"], output=False)"""
        print check_output("sudo ceph osd in 1", shell=True)
        print check_output("sudo start ceph-osd id=1", shell=True)
        # TODO: maybe start measurements before activate?


    def poll_until_health_ok(self):
        while True:
            try:
                ret = check_output(["sudo", "ceph", "health"])
                print ret
                failed = [int(s) for s in ret.split() if s.isdigit()]
                print "failed: " + str(failed)
                if ret.startswith("HEALTH_OK") or \
                        sum([0] + failed) <= 0:
                    break
            except Exception, e:
                print e
            sleep(self.poll_failed_time)

    

    def execute(self):
        print "executing",

        # print args
        args = ""
        for arg in sys.argv:
            args += arg + " "
        print args

        self.experiment_get_ready()

        # start measurements...
        print "starting timer"
        timestart = datetime.utcnow()
        for serv in self.servers:
            try:
                print "running stat_parser.py on " + serv
                stat_parser = Popen("ssh cephuser@" + serv
                        + " '/homes/cephuser/exp/stat_parser.py "
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
                + "-l cephuser -i 'touch /homes/cephuser/exp/done'", shell=True)
      
        # let clients finish
        print "sleep for 3 seconds for clients to finish"
        sleep(3)
        
        self.copy_results_to_master()

        # parse experiment results
        self.parse_results()
        
        self.after_experiment()
        
    def experiment_get_ready(self):
        # clean up
        try:
            call(["rm", "stats.txt", "*.pyc"], output=False)
        except Exception, e:
            print e
              
        print "copying script and dependencies:"
        os.chdir(self.expdir)
        try:
            print check_output("parallel-ssh -h ../servers.txt -l cephuser -i "
                + "'sudo rm -r /homes/cephuser/exp'", shell=True)
        except Exception, e:
            print e
        try:
            print check_output("parallel-ssh -h ../servers.txt -l cephuser -i "
                + "'mkdir /homes/cephuser/exp'", shell=True)
        except Exception, e:
            print e
        try:
            print check_output("parallel-scp -r -l cephuser -h ../servers.txt "
                + self.expdir + "/* /homes/cephuser/exp", shell=True)
        except Exception, e:
            print e

        # if encode, don't waste time waiting for recovery
        if self.experiment.endswith("largefail"):
            # TODO: scp parallel_unmount.sh to secondaries!
            self.poll_until_health_ok()

    def copy_results_to_master(self):
        print "copy stats to this folder"
        os.chdir(self.expdir)
        for serv in self.servers:
            check_output("scp cephuser@" + serv + ":/homes/cephuser/exp/stats.txt" + " ./"
                    + serv + ".txt", shell=True)
        # cleaning up clients
        print "clean client files"
        try:
            check_output("parallel-ssh -h /homes/cephadmin/scripts/servers.txt "
                    + "-l cephuser -i 'rm -r /homes/cephuser/exp 2>/dev/null'"
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
            with open(serv+".txt", "r") as f:
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
                    self.write_measurement_row(csvwriter, "io_p"+str(self.osd_per_node)+"rB",
                            timerow[1]["io"]["sdb2"][0], t, serv)
                    self.write_measurement_row(csvwriter, "io_p"+str(self.osd_per_node)+"wB",
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
        printed_time = (time-self.experiment_start).total_seconds()

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
        else: # all other cases, 1 entry per call
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
                check_output("ssh cephuser@" + serv + " 'sudo stop ceph-osd id="
                        + str(idx) + "'", shell=True)
                check_output("ssh cephuser@" + serv + " 'sudo umount /dev/"
                        + partition + "'", shell=True)
                check_output("ssh cephuser@" + serv + " 'sudo mount -t xfs -o "
                        + "rw,noatime,inode64 /dev/" + partition
                        + " /var/lib/ceph/osd/ceph-" + str(idx) + "'", shell=True)
                check_output("ssh cephuser@" + serv + " 'sudo start ceph-osd id="
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
            #call(["/homes/cephadmin/scripts/repair.py", failed_osd, "2"])
            self.recover_osd()
            #self.poll_until_health_ok()
        elif self.experiment.endswith("encode"):
            pass
    


if __name__ == "__main__":
    p = ParallelPlatform()
    p.execute()
