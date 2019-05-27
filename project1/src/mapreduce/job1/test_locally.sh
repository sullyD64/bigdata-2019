#!/bin/sh
if [ -z "$1" ]
then
    echo "usage: test_locally path_to_file"
    exit 1
fi
cat $1 | python3 mapper.py | sort | python3 reducer.py | column -ts $'\t'