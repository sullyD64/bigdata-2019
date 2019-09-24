import re
import shutil
from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
import os
import utils


PROT = "file://"
ROOTDIR = '<PATH DOVE SALVI I FILE>'
INPUT = '<PATH DATASET INPUT>'
JOIN_INPUT = "/path/to/join_input"
# INPUT = '<PATH DATASET INPUT TEST (più piccolo)'>
# TMPDIR = PATH DOVE SALVARE L'RDD AS TEXT FILE (GENERA UNA DIRECTORY)
TMPDIR = '<PATH STESSO NOME OUTPUT + "-tmp">'
JOIN_TMPDIR = '<PATH STESSO NOME OUTPUT + "-tmp">'
OUTPUT = '<PATH DATASET OUTPUT FINALE>'
JOIN_OUTPUT = '<PATH DATASET OUTPUT JOIN FINALE>'


REG_PATTERN = ""


def map_by_subject(row):
	s, rest = row.split("\t" , 1)
	return (s, rest)

def run_job(rdd , to_join_rdd):

  #creaiamo un rdd che abbia come output le entità distinte di smd 
  to_join_rdd = to_join_rdd.map(lambda x: x.split("\t")[0]).distinct()
  to_join_rdd.repartition(1).saveAsTextFile(PROT + JOIN_OUTPUT)

  #prendiamo dall'altro rdf in input soltanto ciò che ci interessa attraverso una regex, 
  # mappiamo per suggetto della tripla e joiniamo con le entità di smd
  rdd = rdd.filter(lambda x: re.findall(REG_PATTERN, x)).map(map_by_subject).join(to_join_rdd)

  rdd.repartition(1).saveAsTextFile(PROT + OUTPUT)
  # for l in rdd.collect():
  #     print(l)


if __name__ == "__main__":
    spark = utils.create_session("<NOME_SESSIONE>")
    sc = spark.sparkContext

    # rimuovo l'output del precedente job (genera errore se il file è protetto da scrittura,
    # serve a evitare di accorgersene all'ultimo)
    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)

    if os.path.exists(JOIN_OUTPUT):
        os.remove(JOIN_OUTPUT)

    # se ho interrotto il job precedente o sto rilanciando, rimuovo la directory di output
    if os.path.exists(TMPDIR):
        shutil.rmtree(TMPDIR)
        
    if os.path.exists(JOIN_TMPDIR):
        shutil.rmtree(JOIN_TMPDIR)

    # RUNNO IL JOB (spark ha bisogno del protocollo file per caricare l'input)
    rdd = utils.load_data(sc, PROT + INPUT)
    to_join_rdd = utils.load_data(sc, PROT + JOIN_INPUT)

    run_job(rdd , to_join_rdd)
    
    # sposto il file risultato del job fuori dalla TMPDIR e lo rinomino
    shutil.move(TMPDIR + "/part-00000", OUTPUT)
    shutil.move(JOIN_TMPDIR + "/part-00000", JOIN_OUTPUT)
    
    # rimuovo la TMPDIR
    shutil.rmtree(TMPDIR)
    shutil.rmtree(JOIN_TMPDIR)



