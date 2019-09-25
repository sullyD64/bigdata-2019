import os
import re
import shutil
import sys

'''
GENERIC JOB which applies the "Extract Enrichments" pattern:
Input: two datasets, Data and Ext, which are both N-Triples dump with tab-separated "<subject> <predicate> <object>/literal ."
    - Ext contains triples with a given _predicate_
    - Data is the one we want to enrich with the triples in Ext having _predicate_
Output: an Enrichment, which is an N-Triples file with the triples to be added to Data.
    The triples are filtered so that only the triples with the same <subject> in both datasets are added (join).
    If no matching triple was found in Ext for subject <A>, we won't add the triple.
    If a triple in Ext has subject <Z> not present in Data, we won't add the triple.
'''

# Map s,p,o RDF triples to (s,[p,o]) key-value pairs
def extract_subject(row):
    fields = row.split("\t")
    return (fields[0], fields[1:-1])  # ignore trailing dot (last field)


# Compare the predicate with the given one
def is_predicate(row_pred, lookup_pred):
    return row_pred[3][3:-1] == lookup_pred


# Reform the tab-separated string, adding output_pred as predicate of the triple
def reformat_string(row, output_pred):
    return f"{row[0]}\t<{output_pred}>\t{row[1]}\t." # add the dot back


'''
PARAMETERS:(data, ext, lookup_pred, output_pred, cached=False, distinct=False, key_filtering_function=None)
    --> step 1.1: map Data in una coppia (<subject>, None)
    --> step 1.2: filter Data (opzionale, tolgo le chiavi che non mi interessano, <d/t> e <d> nel caso di expected_type)
    --> step 1.3: distinct di Data (opzionale)

    --> step 2.1: map Ext in una coppia (<subject>, _resto_della_tripla_)
    --> step 2: filter_by_regex di Ext (estraggo le triple cercando il predicato lookup_pred)
    --> step 3: map Ext filtrato in una coppia (<subject>, <object>)

    --> step 3: Data.join(Ext)
        (<d/t/p>, (None, <object>))
    --> step 4: map (scarto le coppie dal join estraendo il valore pescato da Ext, riformatto la tripla includendo l'output_pred passato.
    --> step 5: return rdd (non salvarlo qui come file, lo salvo da fuori.)
'''
def run(data, ext, lookup_pred, output_pred, cached=False, distinct=False, key_filtering_func=None):
    if not cached:
        data = data.map(lambda row: (row.split('\t')[0], None))
        if key_filtering_func:
            data = data.filter(key_filtering_func)
        if distinct:
            data = data.distinct()

    ext = ext \
        .map(extract_subject) \
        .filter(lambda row: is_predicate(row[1][0], lookup_pred)) \
        .map(lambda row: (row[0], row[1][1])) \

    result = data.join(ext).map(reformat_string)
    return ([data, result])

if __name__ == "__main__":
    print("This script should not be launched directly: use an enrichment job with spark-submit instead.")
    sys.exit(1)
