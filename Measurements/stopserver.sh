#!/bin/sh
echo "Stopping servers"
ps -ef | grep "[s]erver.jar" | awk '{print $2}' | while read proc; do
   echo killing "$proc"
   kill $proc
done
