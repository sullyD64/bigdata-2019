import sys
import os
from utils import *

filepath = sys.argv[1]
out_path = sys.argv[2]

PREFIX = "http://rdf.freebase.com/ns"
#PREFIX_SUBST = ""
PREFIX_SUBST = "f://b.co"

if not os.path.isfile(filepath):
   print("File path {} does not exist. Exiting...".format(filepath))
   sys.exit()


with open(filepath) as fp:
  cnt = 0
  for line in fp:
   
    if is_triple_allowed(line):
      #clean and save lines on bdpFB 
      cleaned_line = clean_triple(line,PREFIX,PREFIX_SUBST)
      #newline = s+"\t"+p+"\t"+o+"\t."
      save_to_output(out_path, [cleaned_line])
      print(line)









# if not o.startswith("<"):
#       #save line as attribute of label
#       pass

#     el

# if not ontology_exists():
#         dl,tl = generate_ontology(subject, pred, prefix):
#         save_to_output("ontology/file/path", [dl,tl])