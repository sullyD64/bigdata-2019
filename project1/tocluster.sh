#!/bin/sh
if [ -z "$1" ]
then
    echo "usage: tocluster path_to_file"
    exit 1
fi
scp -p 2222 $1 user33@cluster.inf.uniroma3.it:/home/user33