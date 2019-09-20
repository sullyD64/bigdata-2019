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

if os.path.exists(path):
    append_write = 'a' # append if already exists
else:
    append_write = 'w' # make a new file if not

output = open(path,append_write)

with open(filepath) as fp:
  cnt = 0
  for line in fp:
    s,p,o,dot = line.split("\t")    
    if not ontology_exists():
      s,p,o = clean_triple(line,PREFIX,PREFIX_SUBST)
      dl,tl = generate_ontology(s, p, PREFIX_SUBST)
      output.writelines([dl,tl])
      output.close()