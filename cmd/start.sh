#!/bin/bash

num=1
if [ $# -ge 1 ]; then
    num=$1
fi

for i in `seq 1 $num`; do
    port=$((6378 + $i))
    nohup ./bit-server -l $port --db db$i &
done

