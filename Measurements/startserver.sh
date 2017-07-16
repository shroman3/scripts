#!/bin/sh

#echo "Starting server 12300"
#cd "/sraid/server"
#nohup java -jar server.jar 12300 12300 >/dev/null 2>&1 &
#echo "Starting server 12301"
#cd "/sraid2/server"
#nohup java -jar server.jar 12301 12301 >/dev/null 2>&1 &

for i in `seq 1 $1`;
do
	for j in `seq 1 $2`;
		#i=$(($j-1))
		echo "Starting server 123$i$j"
        cd "/shroman/disk$i/sraid$j/server"
        nohup java -jar server.jar 123$i$j 123$i$j >/dev/null 2>&1 &
done

