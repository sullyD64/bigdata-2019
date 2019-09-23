import os
import re
import shutil

import utils

PROT = "file://"
INPUT = '/home/freebase/freebase-s2/freebase-s2'
OUTPUT = '/home/freebase/freebase-s3'

SLICE_IDS = ['-smd', '-type']

SKIP_PATTERNS = "\t<http:[^>]*>\t\."


def run_job(rdd, slice_id):

    rdd.filter(lambda x: not re.findall(SKIP_PATTERNS, x)) \
        .repartition(1) \
        .saveAsTextFile(OUTPUT + slice_id)


if __name__ == "__main__":
    spark = utils.create_session("FB_filtering_https")
    sc = spark.sparkContext

    try:
        shutil.rmtree(OUTPUT)
    except:
        os.mkdir(OUTPUT)

    for slice_id in SLICE_IDS:
        try:
            shutil.rmtree(OUTPUT + slice_id)
        except:
            pass

  # Run the job for each slice produced in s2
    for slice_id in SLICE_IDS:
        rdd = utils.load_data(sc, PROT + INPUT + slice_id)
        run_job(rdd, slice_id)

    for slice_id in SLICE_IDS:
        shutil.move(OUTPUT + slice_id + "/part-00000",
                    OUTPUT + '/freebase-s3' + slice_id)
