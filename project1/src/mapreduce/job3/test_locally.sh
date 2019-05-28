#!/bin/sh
if [ -z "$1" ] || [ -z "$2" ]
then
    echo "usage: test_locally path_to_file1 path_to_file2"
    exit 1
fi
cat "$1" "$2" | python3 firstMapper_join.py | sort | python3 firstReducer_join.py \
        | python3 secondMapper_copy.py | sort | python3 secondReducer.py \
        | python3 thirdMapper.py | sort | python3 thirdReducer.py | column -ts $'\t'
