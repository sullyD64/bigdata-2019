#!/bin/sh
if [ -z "$1" ] || [ -z "$2" ]
then
    echo "usage: test_locally path_to_file1 path_to_file2"
    exit 1
fi
cat "$1" "$2" | python firstMapper_join.py | sort | python firstReducer_join.py \
        | python secondMapper_copy.py | sort | python secondReducer.py | column -ts $'\t'
