import os
import re
import shutil
import json

import utils

PROT = "file://"
ROOT = '/home/freebase/freebase-s3/'
INPUT = ROOT + 'freebase-s32-type'
TMP = ROOT + 'odtp-dict-out'
OUTPUT = ROOT + 'odtp-dict'


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


def to_dict(rdd):
    result = {}
    for item in rdd.items():
        key, values = item[0], list(item[1])
        vals = {}
        for v in list(values):
            vals.update({v[0]: v[1]})
        result.update({key: vals})
    return result

#TODO out of memory 
def run_job(rdd):
    rdd = rdd \
        .filter(filter_odtp) \
        .map(extract_subject) \
        .groupByKey() \
        .repartition(1) \
        .saveAsSequenceFile(OUTPUT)
        # .collectAsMap()

    # with open(OUTPUT, 'w') as output:
    #     json.dump(to_dict(rdd), output)


if __name__ == "__main__":
    spark = utils.create_session("FB_dictionary_builder")
    sc = spark.sparkContext
    
    try:
        shutil.rmtree(TMP)
    except:
        pass

    rdd = utils.load_data(sc, PROT + INPUT)
    run_job(rdd)

    try:
        os.remove(OUTPUT)
        shutil.move(TMP + "/part-00000", OUTPUT)
    except:
        pass

    shutil.rmtree(TMP)
