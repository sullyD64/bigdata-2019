#!/bin/sh
if [ -z "$1" || -z "$2"]
then
    echo "usage: tocluster path_to_file directory"
    exit 1
fi
scp -p 2222 $1 "user33@cluster.inf.uniroma3.it:/home/user33/$2"