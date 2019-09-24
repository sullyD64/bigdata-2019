import os
import re
import shutil
import json

import utils

PROT = "file://"
ROOTDIR = '/home/freebase/freebase-s3/'
INPUT = ROOTDIR + 'freebase-s32-type'
# INPUT = ROOTDIR + 't100'
TMP = ROOTDIR + 'odtp-dict-out'
OUTPUT = ROOTDIR + 'odtp-dict'


# We select only the rows with Predicates of types type.object, type.domain, type.type and type.property.
# Among these, we ignore the following Predicates: type.object.type, type.type.type.instance
PATTERNS = r'^[^\t]+\t<f:type\.' \
    + r'(' \
    + r'object\.type>\t<f:type\.(domain|type|property)>\t\.$|' \
    + r'object(?!\.type)|' \
    + r'domain|' \
    + r'type\.(?!instance)|' \
    + r'property' \
    + r')'


def filter_odtp(row):
    return re.findall(PATTERNS, row)


def extract_subject(row):
    fields = row.split("\t")
    for i, f in enumerate(fields):
        fields[i] = re.sub(r'^<f:([^>]+)>$', r'\1', f)
    return (fields[0], fields[1:3])


def iterate(iterable_list_of_predicates_objects):
    result = {}
    for pred_obj in list(iterable_list_of_predicates_objects):
        result.update({pred_obj[0]: pred_obj[1]})
    return result


def run_job(rdd, sc):
    rdd = rdd \
        .filter(filter_odtp) \
        .map(extract_subject) \
        .groupByKey() \
        .mapValues(iterate) \
        .repartition(1) \
        .saveAsSequenceFile(TMP)

    shutil.move(TMP + "/part-00000", OUTPUT + '-tmp')
    shutil.rmtree(TMP)

    with open(OUTPUT + '.json', 'w') as output:
        temp = sc.sequenceFile(OUTPUT+'-tmp').collectAsMap()
        json.dump(temp, output)
        os.remove(OUTPUT + '-tmp')


if __name__ == "__main__":
    spark = utils.create_session("FB_dictionary_builder")
    sc = spark.sparkContext

    try:
        shutil.rmtree(OUTPUT + '.json')
    except:
        pass

    try:
        shutil.rmtree(TMP)
    except:
        pass

    rdd = utils.load_data(sc, PROT + INPUT)
    run_job(rdd, sc)
