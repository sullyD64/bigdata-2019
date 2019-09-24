import os
import shutil
from rdflib import Graph, URIRef

'''
WARNING: THIS IS A SLOW SCRIPT (not a Spark job!)
'''

ROOTDIR = '/home/freebase/freebase-s4/'
# TTI and TOT are slices of freebase-s3-type, extracted using UNIX grep:
# TTI contains all triples with Predicate type.type.instance (30,9% of total triples)
# TOT contains all triples with Predicate type.object.type (6,2% of total triples)
TTI = 'fbtype__type-type-instance'
TOT = 'fbtype__type-object-type'

OUT = 'tti_reference'
CUSTOM_PREDICATE = 'fbo:type'

tti = Graph(store='Sleepycat', identifier='type-instance-map')
tot = Graph(store='Sleepycat', identifier='isa-to-add')


def create_graphs():
    print("Creating graphs...")
    tti.open(ROOTDIR + 'tti_graph', create=True)
    tti.parse(ROOTDIR + TTI, format="nt")
    tti.close()

    tot.open(ROOTDIR + 'tot_graph', create=True)
    tot.parse(ROOTDIR + TOT, format="nt")
    tot.close()
    print("Done.")


def load_graphs():
    tti.open(ROOTDIR + 'tti_graph')
    tot.open(ROOTDIR + 'tot_graph')


def join_graphs():
    print("Reversing object-type triples into type-instance and adding them to TTI...")
    for s, p, o in tot:
        tti.add((o, URIRef("f:type.type.instance"), s))
    print("Done.")


def delete_graphs():
    print(f"Deleting graphs...")
    tti.close()
    tot.close()
    # we keep tti as it offers a reference for building the type-instance-map.
    # shutil.rmtree(ROOTDIR + 'tti_graph')
    shutil.rmtree(ROOTDIR + 'tot_graph')
    print("Done.")


if __name__ == "__main__":
    create_graphs()
    load_graphs()

    # join discarding duplicates
    initial_len = len(tti)
    join_graphs()
    delta = len(tti) - initial_len
    print(f"{delta} triples were added.")
    print("TOT *IS NOT* a subset of TTI!" if delta >
          0 else "TOT *IS* a subset of TTI!")

    # save the resulting graph to output file
    if os.path.exists(OUT):
        os.remove(OUT)

    print(f"Saving output to: {OUT}...")
    output = open(ROOTDIR + OUT, 'w')
    for s, p, o in tti:
        output.write(f"<{o}>\t<{CUSTOM_PREDICATE}>\t<{s}>\t.\n")
    output.close()
    print(f"Sorting {OUT}...")
    os.system(f"sort {OUT} -o {OUT}")

    # we only delete TOT
    delete_graphs()