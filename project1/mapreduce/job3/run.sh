#!/bin/sh
cat ../../testdata/joininput/* | py firstMapper_join.py | sort | py firstReducer_join.py | py secondMapper_copy.py | sort | py secondReducer.py | py thirdMapper.py | sort | py thirdReducer.py | column -ts $'\t'