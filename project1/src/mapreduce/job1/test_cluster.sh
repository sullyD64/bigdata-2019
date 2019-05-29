#!/bin/sh
if [ -z "$1" ]
then
    echo "usage: test_locally path_to_file"
    exit 1
fi
cat $1 | python mapper.py | sort | python reducer.py | column -ts $'\t'
