#!/bin/bash


ITER="1"
SIZE="100000 500000 1000000"
PRELOAD="0 1000000"
python3 cli.py prepare
for i in $SIZE
do
    for j in $PRELOAD
    do
    	echo "------------------------------------------------------"
    	echo "RUN python3 cli.py run -i "$ITER "-n" $i "-p" $j
    	echo "------------------------------------------------------"
    	python3 cli.py run -i $ITER -n $i -p $j
  	echo "------------------------------------------------------"
 	echo "python3 cli.py run -i "$ITER "-n" $i "-p" $j "finished"
   	echo "------------------------------------------------------"
   	echo
   	echo
    done
done
