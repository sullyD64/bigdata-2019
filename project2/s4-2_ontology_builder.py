import os
import shutil
from rdflib import Graph, Namespace, URIRef

ROOTDIR = '/home/freebase/freebase-s4/'

TIM = 'type_instance_map'

tim = Graph(store='Sleepycat', identifier='isa-to-add')
onto = Graph(store='Sleepycat', identifier='ontology')

prefixes = {
    "fbo": "fbo",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",  
}


# @prefix : <http://localhost/ontologies/2019/1/10/automobile#> .
# @prefix owl: <http://www.w3.org/2002/07/owl#> .
# @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
# @prefix xml: <http://www.w3.org/XML/1998/namespace> .
# @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
# @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
# @base <http://localhost/ontologies/2019/1/10/automobile> .


def create_graphs():
    pass


def load_graphs():
    pass


if __name__ == "__main__":
    pass
