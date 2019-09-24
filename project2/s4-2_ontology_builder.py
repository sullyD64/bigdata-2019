import os
import sys
import logging
import shutil
import json
import re
from rdflib import Graph, URIRef as URI, Literal, Namespace, RDF, RDFS, OWL

file_handler = logging.FileHandler(filename='ontology-builder.log')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]
logging.basicConfig(
    level=logging.INFO,
    # format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    format='[%(asctime)s] %(levelname)s - %(message)s',
    handlers=handlers
)
logger = logging.getLogger()

ROOTDIR = '/home/freebase/freebase-s4/'
SMD_path = '/home/freebase/freebase-s3/freebase-s3-smd'
ODTP_path = '/home/freebase/freebase-s3/odtp-dict.json'
# ODTP_path = '/home/freebase/freebase-s3/odtptest.json'

TTI = Graph(store='Sleepycat', identifier='type-instance-map')
ONTO = Graph(store='Sleepycat', identifier='ontology')
TIM = Graph(store='Sleepycat', identifier='tim')

ONTO_DIR = ROOTDIR + 'ontology_graph'
TIM_DIR = ROOTDIR + 'tim_graph'

FB = Namespace('f:')
FBO = Namespace('fbo:')
FB_EXPECTED_TYPE = "type.property.expected_type"
FB_NAME = "type.object.name"

OUT_ONTO = ROOTDIR + 'freebase-ontology'
OUT_TIM = ROOTDIR + 'freebase-type_instance_map'


# def test():
#     i = 0
#     for s, p, o in TIM:
#         if i < 10:
#             logger.info(f"<{s}>\t<{p}>\t <{o}>\t.")
#         else:
#             break
#         i = i+1


def create_graphs():
    logger.info("Creating graphs...")
    if os.path.exists(ONTO_DIR):
        shutil.rmtree(ONTO_DIR)
    ONTO.open(ONTO_DIR, create=True)
    for triple in ONTO:
        ONTO.remove(triple)
    ONTO.close()

    if os.path.exists(TIM_DIR):
        shutil.rmtree(TIM_DIR)
    TIM.open(TIM_DIR, create=True)
    for triple in TIM:
        TIM.remove(triple)
    TIM.close()
    logger.info("Done.")


def load_graphs():
    ONTO.open(ONTO_DIR)
    TIM.open(TIM_DIR)


def load_dictionaries():
    logger.info("Loading references & dictionaries...")
    logger.info("Loading TTI...")
    TTI.open(ROOTDIR + 'tti_graph')
    logger.info("Done.")
    logger.info("Loading ODTP...")
    odtp_file = open(ODTP_path)
    ODTP = json.load(odtp_file)
    logger.info("Done.")
    return ODTP


def close_graphs():
    logger.info("Closing graphs...")
    ONTO.close()
    TIM.close()
    TTI.close()


if __name__ == "__main__":
    create_graphs()
    load_graphs()
    ODTP = load_dictionaries()

    # test()
    with open(SMD_path) as infile:

        # head = [next(infile) for x in range(25)]  # test
        # for i, triple in enumerate(head):

        # MAIN LOOP
        logger.info("==== BEGIN LOOP OVER SMD")
        for i, triple in enumerate(infile):
            tokens = triple.split("\t")
            subj, pred, obj = tokens[0], tokens[1], tokens[2]
            pred = pred[3:-1].split(".")

            if len(pred) < 3:
                continue

            fdomain = f"/{pred[0]}"
            ftype = f"/{pred[0]}/{pred[1]}"
            fprop = f"/{pred[0]}/{pred[1]}/{pred[2]}"

            udomain = f"{pred[0]}"
            utype = f"{pred[0]}.{pred[1]}"
            uprop = f"{pred[0]}.{pred[1]}.{pred[2]}"

            # domain
            add_dclass = (URI(FBO+udomain), RDF.type, RDFS.Class)
            # type (type is subclass of domain)
            add_tclass = (URI(FBO+utype), RDF.type, RDFS.Class)
            add_tsco = (URI(FBO+utype), RDFS.subClassOf, URI(FBO+udomain))

            # property (either a relationship or a literal property)
            add_ptype = None
            if (obj.startswith('<')):
                add_ptype = (URI(FBO+uprop), RDF.type, OWL.ObjectProperty)
            else:
                add_ptype = (URI(FBO+uprop), RDF.type, OWL.DatatypeProperty)
            # property domain
            add_pdomain = (URI(FBO+uprop), RDFS.domain, URI(FBO+utype))

            # property range
            add_prange = None
            add_exptype_domain_class = None
            add_exptype_class = None
            add_exptype_sco = None
            # check if the property is already defined before looking for expected type
            if (URI(FBO+uprop), None, None) not in ONTO:
                try:
                    odtp_exptype = ODTP[uprop][FB_EXPECTED_TYPE]
                    if not re.findall(r"^[mg]\.", odtp_exptype):
                        # eagerly add type and domain of the newfound expected type
                        odtp_exptype_domain = odtp_exptype.split('.')[0]
                        add_exptype_domain_class = (URI(FBO+odtp_exptype_domain), RDF.type, RDFS.Class)
                        add_exptype_class = (URI(FBO+odtp_exptype), RDF.type, RDFS.Class)
                        add_exptype_sco = (URI(FBO+odtp_exptype), RDFS.subClassOf, URI(FBO+odtp_exptype_domain))
                        add_prange = (URI(FBO+uprop), RDFS.range, URI(FBO+odtp_exptype))
                except KeyError:  # property not found
                    pass

            # labels (to improve readability)
            add_dlabel = (URI(FBO+udomain), RDFS.label, Literal(fdomain))
            add_tlabel = (URI(FBO+utype), RDFS.label, Literal(ftype))
            add_plabel = (URI(FBO+uprop), RDFS.label, Literal(fprop))
            # look for better labels if available
            try:
                odtp_dlabel = ODTP[udomain][FB_NAME]
                add_dlabel = (URI(FBO+udomain), RDFS.label, Literal(odtp_dlabel))
            except KeyError:  # label not found
                pass
            try:
                odtp_tlabel = ODTP[utype][FB_NAME]
                add_tlabel = (URI(FBO+utype), RDFS.label, Literal(odtp_tlabel))
            except KeyError:  # label not found
                pass
            try:
                odtp_plabel = ODTP[uprop][FB_NAME]
                add_plabel = (URI(FBO+uprop), RDFS.label, Literal(odtp_plabel))
            except KeyError:  # label not found
                pass

            # ADD TRIPLES TO ONTOLOGY
            initial_length = len(ONTO)
            ONTO.add(add_dclass)
            ONTO.add(add_tclass)
            ONTO.add(add_tsco)
            if add_ptype:
                ONTO.add(add_ptype)
            ONTO.add(add_pdomain)
            if add_prange:
                ONTO.add(add_prange)
            if add_exptype_domain_class:
                ONTO.add(add_exptype_domain_class)
            if add_exptype_class:
                ONTO.add(add_exptype_class)
            if add_exptype_sco:
                ONTO.add(add_exptype_sco)
            ONTO.add(add_dlabel)
            ONTO.add(add_tlabel)
            ONTO.add(add_plabel)
            delta = len(ONTO) - initial_length

            # type-instance-mapping for subject and object
            # we user our custom predicate, FBO:type, to establish a link between ontology and instances.
            add_subjtype = (URI(FB+subj[3:-1]), FBO.type, URI(FBO+utype))
            add_objtype = None
            if obj.startswith('<'):
                obj = obj[3:-1]
                if (URI(FB+obj), None, None) not in TIM:
                    # eagerly look for object's type in TTI if it's not a literal, so that we are sure that it will have a type
                    # (otherwise if the mid is not subject in any triple, it will be orphan of a type)
                    try:
                        obj_type = list(TTI.triples((None, None, (URI(FB+obj)))))[0][0][2:]
                        add_objtype = (URI(FB+obj), FBO.type, URI(FBO+obj_type))
                    except:
                        pass

            # ADD TRIPLES TO TIM
            initial_length = len(TIM)
            TIM.add(add_subjtype)
            if add_objtype:
                TIM.add(add_objtype)
            delta = len(TIM) - initial_length
            logger.info(f"#{i+1} | [ONTO] - {delta} triples added. | [TIM] - {delta} triples added.")
        logger.info("==== LOOP OVER SMD COMPLETED")
    
    # for s, p, o in ONTO:
    #     print(f"<{s}>\t<{p}>\t<{o}>\t.")
    # print("="*100)
    # for s, p, o in TIM:
    #     print(f"<{s}>\t<{p}>\t<{o}>\t.")

    # SAVE OUTPUT
    if os.path.exists(OUT_ONTO):
        os.remove(OUT_ONTO)
    if os.path.exists(OUT_TIM):
        os.remove(OUT_TIM)

    logger.info(f"Saving Ontology output to: {OUT_ONTO}...")
    output = open(OUT_ONTO, 'w')
    for s, p, o in ONTO:
        if isinstance(o, Literal):
            output.write(f'<{s}>\t<{p}>\t"{o}"\t.\n')
        else:
            output.write(f'<{s}>\t<{p}>\t<{o}>\t.\n')
    output.close()
    logger.info(f"Done.")

    logger.info(f"Saving Ontology output to: {OUT_TIM}...")
    output = open(OUT_TIM, 'w')
    for s, p, o in TIM:
        output.write(f'<{s}>\t<{p}>\t<{o}>\t.\n')
    output.close()
    logger.info(f"Done.")

    
    close_graphs()
    logger.info("JOB COMPLETED :)")
