#!/bin/bash

for j in `seq 1 $1`;
do
	i=$(($j-1))
	echo "Starting server 1230$i"
        cd "/dev/shm/sraid/server$i"
        nohup java -jar server.jar 1230$i 1230$i >/dev/null 2>&1 &
done
