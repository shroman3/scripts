#!/usr/bin/python
import os
from subprocess import check_output


def parallelscp(copy_from, copy_to, write=False):
    try:
        out = check_output("sshpass -f p.txt parallel-scp -A -h ../servers.txt "
            + copy_from + " " + copy_to + " < p.txt", shell=True)
        if (write and out):
            print out
    except Exception, e:
        if write: 
            print e

def prepare_servers():
    os.chdir("/home/shroman/sraid/scripts")
    for disk in range(1, 5):
        print "Copy for disk " + str(disk)
        for i in range(1, 9):
            print "    proc " + str(i)
            parallelscp("../server/server.jar", "/shroman/disk" + str(disk) + "/sraid" + str(i) + "/server/server.jar")


if __name__ == "__main__":
    prepare_servers()
