#!/usr/bin/python
import os
from subprocess import check_output, STDOUT


def parallelscp(copy_from, copy_to, write=False):
    try:
        out = check_output("sshpass -f p.txt parallel-scp -A -h ../servers.txt "
            + copy_from + " " + copy_to + " < p.txt", shell=True)
        if (write and out):
            print out
    except Exception, e:
        if write: 
            print e
            
def run_command(command, write=False):
    try:
        out = check_output(command, shell=True, stderr=STDOUT)
        if (write and out):
            print out
    except Exception as e:
        if write: 
            print e
            
def prepare_servers():
    os.chdir("/home/shroman/sraid/scripts")
    print "preparing CLIENT"
    run_command("cp ../client/* /shroman/disk1/sraid1/client/")
    run_command("cp ../server/* /shroman/disk1/sraid1/server/")
    print "preparing SERVERS"
    for disk in range(1, 5):
        print "Copy for disk " + str(disk)
        for i in range(1, 9):
            print "    proc " + str(i)
            parallelscp("../server/server.jar", "/shroman/disk" + str(disk) + "/sraid" + str(i) + "/server/server.jar")
            parallelscp("../server/sraid.store", "/shroman/disk" + str(disk) + "/sraid" + str(i) + "/server/sraid.store")


if __name__ == "__main__":
    prepare_servers()
