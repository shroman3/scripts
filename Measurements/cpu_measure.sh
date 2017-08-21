#!/bin/bash
# by Matan Liram

if [ "$1" == "all" ]; then
  for i in {1..32}; do
    CPU=($(cat /proc/stat | grep "^cpu$((i-1)) ")) # Get the total CPU statistics.
    unset CPU[0]                          # Discard the "cpu" prefix.
    IDLE=${CPU[4]}                        # Get the idle CPU time.
    
    # Calculate the total CPU time.
    TOTAL=0
    
    for VALUE in "${CPU[@]:0:4}"; do
      let "TOTAL=$TOTAL+$VALUE"
    done
    echo -ne "cpu${i}:${TOTAL},${IDLE}\n";
  
  done
else
  CPU=($(cat /proc/stat | grep "^cpu ")) # Get the total CPU statistics.
  unset CPU[0]                          # Discard the "cpu" prefix.
  IDLE=${CPU[4]}                        # Get the idle CPU time.
  
  # Calculate the total CPU time.
  TOTAL=0
  
  for VALUE in "${CPU[@]:0:4}"; do
    let "TOTAL=$TOTAL+$VALUE"
  done
  echo -ne "cpu:${TOTAL},${IDLE}\n";
fi
# Calculate the CPU usage since we last checked.
# let "DIFF_IDLE=$IDLE-$PREV_IDLE"
# let "DIFF_TOTAL=$TOTAL-$PREV_TOTAL"
# let "DIFF_USAGE=(1000*($DIFF_TOTAL-$DIFF_IDLE)/$DIFF_TOTAL+5)/10"
# echo -en "\rCPU: $DIFF_USAGE%  \b\b"
