import operator
import sys
import random
from munkres import Munkres, make_cost_matrix, print_matrix
from time import time
import requests
import json
import sys
import numpy as np
import ujson
from copy import deepcopy
from requests.exceptions import ConnectionError
from concepts import Context

reload(sys)
sys.setdefaultencoding("utf-8")

class MFuhsionPerfect:

    """
    FuhSen Join operator with 1-1 perfect matching
    similarityMatrix - the matrix with similarities
    threshold - float value in [0,1]
    table1 - a table which belongs to resource triple list 1
    table2 - a table which belongs to resource triple list 2
    toBeJoined - a list with produced join results
    computedJoins - a list of already computed joins, not to repeat already found results
    isRow - whether a join argument is located in the row or column of the similarity matrix
    """

    def __init__(self, threshold, simfunction, dataset_size):
        # self.similarityMatrix = similarity
        self.threshold = threshold
        self.table1 = []
        self.table2 = []
        self.toBeJoined = []
        self.toBeJoinedFca = []
        self.computedJoins = []
        self.result_matrix = {}
        self.simfunction = simfunction
        self.batchSize = 100

        self.left_table = []
        self.right_table = []
        self.total_op_time = 0
        self.total_sim_time = 0
        self.total_fca_time = 0
        self.dataset_size = dataset_size
        self.context = {"http://dbpedia.org/ontology/occupation": None}

    def execute_new(self, left_rtl_list, right_rtl_list):
        start_op_time = time()

        # insert and probe
        self.left = left_rtl_list
        self.right = right_rtl_list

        self.insertIntoTable(self.left, self.left_table)
        self.insertIntoTable(self.right, self.right_table)

        self.probeTables(self.left_table, self.right_table)

        finish_op_time = time()
        self.total_op_time = finish_op_time - start_op_time

    def insertIntoTable(self, list_rtls, table):
        for rtl in list_rtls:
            if rtl['head']['uri'] not in table:
                table.append(rtl['head']['uri'])

    def probeTables(self, left_table, right_table):
        # initialize the similarity matrix
        if len(left_table) > 0 and len(right_table) > 0:
            simmatrix = np.zeros((len(left_table), len(right_table)), float)

            start_sim_time = time()
            for i in xrange(0,len(left_table),self.batchSize):
                for j in xrange(0, len(right_table), self.batchSize):
                    if i+self.batchSize<=len(left_table) and j+self.batchSize<=len(right_table):
                        add_matrix = self.sim_batch(left_table[i:i+self.batchSize],right_table[j:j+self.batchSize])
                        simmatrix[i:i+self.batchSize, j:j+self.batchSize] = add_matrix
                    elif i+self.batchSize<=len(left_table) and j+self.batchSize>len(right_table):
                        add_matrix = self.sim_batch(left_table[i:i+self.batchSize], right_table[j:len(right_table)])
                        simmatrix[i:i + self.batchSize, j:len(right_table)] = add_matrix
                    elif i+self.batchSize>len(left_table) and j+self.batchSize<=len(right_table):
                        add_matrix = self.sim_batch(left_table[i:len(left_table)], right_table[j:j+self.batchSize])
                        simmatrix[i:len(left_table), j:j + self.batchSize] = add_matrix
                    else:
                        add_matrix = self.sim_batch(left_table[i:len(left_table)], right_table[j:len(right_table)])
                        simmatrix[i:len(left_table), j:len(right_table)] = add_matrix
            finish_sim_time = time()
            print "Similarity Matrix Created"
            print time()
            self.total_sim_time += finish_sim_time - start_sim_time
            test_threshold = np.percentile(simmatrix, self.threshold)


            # pruning
            simmatrix[simmatrix < test_threshold] = 0
            backup_matrix = deepcopy(simmatrix)

            print "MINTE matching"
            print time()
            # run hungarian algorithm to find 1-1 perfect match
            cost_matrix = make_cost_matrix(simmatrix.tolist(), lambda cost: 1.0 - cost)
            m = Munkres()
            perfect_indices = m.compute(cost_matrix)

            for a, b in perfect_indices:
                self.toBeJoined.append((left_table[a], right_table[b]))
                # print left_table[a] + "-----" + right_table[b]
            print "MINTE complete"
            print time()

            print "COMET FCA matching"
            print time()
            simmatrix = backup_matrix
            start_fca_time = time()
            # context filtering
            # if self.runFCA:
            simmatrix = self.applyFCA(simmatrix)
            end_fca_time = time()
            self.total_fca_time += end_fca_time - start_fca_time
            print "FCA complete"
            print time()

            cost_matrix = make_cost_matrix(simmatrix.tolist(), lambda cost: 1.0 - cost)
            perfect_indices = m.compute(cost_matrix)

            for a, b in perfect_indices:
                self.toBeJoinedFca.append((left_table[a], right_table[b]))
                # print left_table[a] + "-----" + right_table[b]


    def applyFCA(self, simmatrix):
        for rowId, row in enumerate(simmatrix):
            for columnId, element in enumerate(row):
                if element > 0:
                    simmatrix[rowId][columnId] = element * self.localFca([self.left[rowId], self.right[columnId]], self.context)
                # if self.left[rowId]['head']['uri'] == "http://dbpedia.org/resource/Ben_Wyatt_d1_actor2" and self.right[columnId]['head']['uri'] == "http://dbpedia.org/resource/Ben_Wyatt_d2_actor1":
                #     rid = rowId
                #     cid = columnId
        return simmatrix

    def localFca(self, molecules, context):
        contextItems = []
        for prop in context:
            # when context is of the form "x property must match" we must consider all the values of this property
            if context[prop] is None:
                for tail in molecules[0]['tail']:
                    if tail['prop'] == prop:
                        contextItems.append((tail['prop'], tail['value']))
            else:
                contextItems.append((prop, context[prop]))

        mat = []
        for molecule in molecules:
            row = [False] * len(contextItems)
            for idx, contextItem in enumerate(contextItems):
                for tail in molecule['tail']:
                    if tail['prop'] == contextItem[0] and tail['value'] == contextItem[1]:
                        row[idx] = True
            mat.append(row)

        contextProperties = [prop + '->' + value for prop, value in contextItems]
        moleculeNames = [molecule['head']['uri'] + '_' + str(idx) for idx, molecule in enumerate(molecules)]

        c = Context(moleculeNames, contextProperties, mat)
        res = c.intension(moleculeNames)

        if len(res) > 0:
            return True
        return False

    def sim(self, uri1, uri2):
        url = "http://localhost:9000/similarity/"+self.simfunction
        data = {"tasks": [{"uri1": uri1[1:-1], "uri2": uri2[1:-1]}]}
        headers = {'content-type': "application/json"}
        response = requests.post(url, data=json.dumps(data), headers=headers)
        resp_object = json.loads(response.text)
        return resp_object[0]["value"]

    def sim_batch(self, list_uris1, list_uris2):
        url = "http://localhost:9000/similarity/" + self.simfunction+"?minimal=true"
        headers = {'content-type': "application/json"}
        data = {"tasks": []}
        for i in xrange(len(list_uris1)):
            for j in xrange(len(list_uris2)):
                data["tasks"].append({"uri1":list_uris1[i].strip(), "uri2":list_uris2[j].strip()})
        # print json.dumps(data, ensure_ascii=False)
        response = requests.post(url, data=json.dumps(data, ensure_ascii=False), headers=headers)
        print response
        resp_object = ujson.loads(response.text)
        # results = [[0 for k in xrange(self.batchSize)] for l in xrange(self.batchSize)]
        results = np.zeros((len(list_uris1),len(list_uris2)), float)
        i = 0
        for k in xrange(len(resp_object)):
            j = k % self.batchSize
            results[i][j] = resp_object[k]['value']
            i = i if (j < self.batchSize-1) else i+1
        return results

    def transformDictToArray(self):
        indexes = sorted(self.result_matrix, key=operator.itemgetter(0, 1))

        # take the last element to find the size of the array
        i, j = indexes[-1]
        result = [[0 for x in range(j+1)] for y in range(i+1)]
        for (a, b) in indexes:
            result[a][b] = self.result_matrix[(a, b)]

        return result

    def computePerfectMatching(self):
        # transform dict to array
        inputMatrix = self.transformDictToArray()

        # compute perfect matching
        cost_matrix = make_cost_matrix(inputMatrix, lambda cost: sys.maxsize - cost)
        m = Munkres()
        perfect_indexes = m.compute(cost_matrix)

        return perfect_indexes
