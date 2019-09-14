import re
import os

def is_triple_allowed(line):
	#se passa attraverso tutte le regex ritorna True, altrimenti False
	#REGEX = "w3\.org|@(?!en)|<http://rdf\.freebase\.com/ns/type\."
	# #+"/type[\./](?!object\.name)|"\
	SKIP_PATTERNS = "w3\.org|"\
	+"@(?!en)|"\
	+"/type\.|"\
	+"/user.*|"\
	+"/freebase\.(?!domain_category).*|"\
	+"/usergroup\.|"\
	+"/permission\.|"\
	+"/community\.|"\
	+"/common\.(?!document|topic)\\b.*|"\
	+"/common\.document\.(?!source_uri)\b.*|"\
	+"/common\.topic\.(description|image|webpage|properties|weblink|notable_for|article).*|"\
	+"/dataworld\.|"\
	+"/key/.*|"\
	+"/base\."
	

	return not re.findall(SKIP_PATTERNS , line)

def ontology_exists():
	#controlla nel file di output se esiste e controlla se è type.object.name
	return True

def generate_ontology(subject, pred, prefix):
	#dalla tripla genera le triple relative al dominio e al tipo in riferimento al soggetto
	pred_split = pred.split(".")
	domain_line = subject+"\t<"+prefix+"has_domain>\t"+pred_split[0]
	type_line = subject+"\t<"+prefix+"instance_of>\t"+pred_split[1]

	return [domain_line, type_line]

def clean_triple(line, prefix, subst):

	return line.replace(prefix,subst)


def save_to_output(path, lines):

	if os.path.exists(path):
	    append_write = 'a' # append if already exists
	else:
	    append_write = 'w' # make a new file if not

	output = open(path,append_write)
	output.writelines(lines)
	output.close()

