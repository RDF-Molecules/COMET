from perfect_matching import MFuhsionPerfect
import codecs
import json
from time import time
from rdflib import Graph, URIRef

def toString(s):
    return u''.join(s).encode('ascii','ignore').strip()

goldStandardPairs = []
simfunction = "gades"

def prepareGSForCOMET(path):
    with codecs.open(path) as f:
        for line in f:
            name1 = line.split(">,<")[0].strip()[1:]
            name2 = line.split(">,<")[1].strip()[:-1]
            name1 = u'' + name1
            name1 = name1.encode('ascii', 'ignore')
            name2 = u'' + name2
            name2 = name2.encode('ascii', 'ignore')
            goldStandardPairs.append((name1, name2))
    print goldStandardPairs

def testMFuhsionPerfect(path1, path2, threshold):
    print "Semantic Join Blocking Perfect Operator"
    print time()
    dbp_rtls_path = path1
    drugbank_rtl_path = path2
    dbp_rtls = []
    drugbank_rtls = []
    print "Reading Dataset_1", path1
    with codecs.open(dbp_rtls_path, "r", encoding='utf-8') as f:
        for line in f:
            newline = line.decode("utf-8").replace('\"row\": true', '\"row\":True')
            dbp_rtls.append(eval(newline))
    print "Reading Dataset_2", path2
    with codecs.open(drugbank_rtl_path, "r", encoding='utf-8') as f2:
        for line in f2:
            newline = line.decode("utf-8").replace('\"row\": false', '\"row\":False')
            drugbank_rtls.append(eval(newline))
    print "Executing Join"
    print "Creating Similarity Matrix"
    print time()
    perfectOp = MFuhsionPerfect(threshold, simfunction, dataset_size)
    perfectOp.execute_new(dbp_rtls, drugbank_rtls)
    # for a,b in perfectOp.toBeJoined:
    #     print "Identified joins: ",a," --- ",b

    print "MINTE: Semantic Join Blocking Perfect Operator"
    print "Overall ",str(len(perfectOp.toBeJoined))," pairs"
    print "Total operator time ",perfectOp.total_op_time, "seconds including ",perfectOp.total_sim_time,"seconds for similarity and ", perfectOp.total_fca_time, "seconds for FCA"
    print "Clean time is ",perfectOp.total_op_time - (perfectOp.total_sim_time + perfectOp.total_fca_time), "seconds"
    # compute precision and recall
    intersection = 0
    for a, b in perfectOp.toBeJoined:
        if ((a, b) in goldStandardPairs) or ((b, a) in goldStandardPairs):
            intersection += 1
    precision = float(intersection) / len(perfectOp.toBeJoined)
    recall = float(intersection) / len(goldStandardPairs)

    print "Threshold Percentile", threshold
    print "Precision", precision
    print "Recall", recall
    print "-----"

    print "COMET: Semantic Join Blocking Perfect Operator"
    print "Overall ", str(len(perfectOp.toBeJoinedFca)), " pairs"
    # compute precision and recall
    intersection = 0
    for a, b in perfectOp.toBeJoinedFca:
        if ((a, b) in goldStandardPairs) or ((b, a) in goldStandardPairs):
            intersection += 1
    precision = float(intersection) / len(perfectOp.toBeJoinedFca)
    recall = float(intersection) / len(goldStandardPairs)

    print "Threshold Percentile", threshold
    print "Precision", precision
    print "Recall", recall
    print "-----"

"""
    COMET EXPERIMENT
    Preparatory steps:
        (a) Generate data and Gold Standard according to configuration
        (b) Read RDF files into local objects
    Run Experiment:
        (a) Read the subjects from Gold Standard and store locally
"""
# dataset_size = number of distinct molecules in dataset
dataset_size = 1000

# configuration: a string with the format "x-y"
# where x, y are integers and
# x = number of similar molecules in the same context,
# y = number of similar molecules with different context
configuration = "2-2"

threshold = 97

#  Run Experiment:
# (a) Read the subjects from Gold Standard and store locally
prepareGSForCOMET("context_evaluation/" + str(dataset_size) + "/" + configuration + "/gs_" + str(dataset_size) + ".txt")

# run MINTE and COMET
runFCA = True
testMFuhsionPerfect("context_evaluation/" + str(dataset_size) + "/" + configuration + "/d1_rtl.txt","context_evaluation/" + str(dataset_size) + "/" + configuration + "/d2_rtl.txt", threshold)