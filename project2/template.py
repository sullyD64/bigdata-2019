import os
import shutil

import utils

PROT = "file://"
ROOTDIR = '<PATH DOVE SALVI I FILE>'
INPUT = '<PATH DATASET INPUT>'
# INPUT = '<PATH DATASET INPUT TEST (più piccolo)'>
# TMPDIR = PATH DOVE SALVARE L'RDD AS TEXT FILE (GENERA UNA DIRECTORY)
TMPDIR = '<PATH STESSO NOME OUTPUT + "-tmp">'
OUTPUT = '<PATH DATASET OUTPUT FINALE>'

def run_job(rdd):
    # rdd = rdd \
    # OPERAZIONI SU RDD
    #     .saveAsSequenceFile(TMPDIR)

if __name__ == "__main__":
    spark = utils.create_session("<FB_JOB-ID>")
    sc = spark.sparkContext

    # rimuovo l'output del precedente job (genera errore se il file è protetto da scrittura,
    # serve a evitare di accorgersene all'ultimo)
    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)

    # se ho interrotto il job precedente o sto rilanciando, rimuovo la directory di output
    if os.path.exists(TMPDIR):
        shutil.rmtree(TMPDIR)

    # RUNNO IL JOB (spark ha bisogno del protocollo file per caricare l'input)
    rdd = utils.load_data(sc, PROT + INPUT)
    run_job(rdd)
    
    # sposto il file risultato del job fuori dalla TMPDIR e lo rinomino
    shutil.move(TMPDIR + "/part-00000", OUTPUT)
    # rimuovo la TMPDIR
    shutil.rmtree(TMPDIR)
