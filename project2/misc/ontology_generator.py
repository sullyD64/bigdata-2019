import sys
import os
from utils import *

filepath = sys.argv[1]
out_path = sys.argv[2]

PREFIX = ""
PREFIX_SUBST = ""

if not os.path.isfile(filepath):
   print("File path {} does not exist. Exiting...".format(filepath))
   sys.exit()



with open(filepath) as fp:
  cnt = 0
  for line in fp:
    s,p,o,dot = line.split("\t")    
    if not ontology_exists():
      s,p,o = clean_triple(line,PREFIX,PREFIX_SUBST)
      dl,tl = generate_ontology(s, p, PREFIX_SUBST)
      save_to_output(out_path, [dl,tl])