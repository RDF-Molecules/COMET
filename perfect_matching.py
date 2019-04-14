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
            # simmatrix = [[0 for i in xrange(len(right_table))] for j in xrange(len(left_table))]
            simmatrix = np.zeros((len(left_table), len(right_table)), float)

            # for i in xrange(len(left_table)):
            #     for j in xrange(len(right_table)):
            #         start_sim_time = time()
            #
            #         simscore = self.sim(left_table[i],right_table[j])
            #
            #         finish_sim_time = time()
            #         self.total_sim_time += finish_sim_time - start_sim_time
            #
            #         if simscore >= self.threshold:
            #             simmatrix[i][j] = simscore
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

    def fca(self, molecules, context):
        contextItems = []
        for prop in context:
            #when context is of the form "x property must match" we must consider all the values of this property
            if context[prop] is None:
                for tail in molecules[0]['tail']:
                    if tail['prop'] == prop:
                        contextItems.append((tail['prop'], tail['value']))
            else:
                contextItems.append((prop, context[prop]))

        mat = []
        for molecule in molecules:
            row = [0] * len(contextItems)
            for idx, contextItem in enumerate(contextItems):
                for tail in molecule['tail']:
                    if tail['prop'] == contextItem[0] and tail['value'] == contextItem[1]:
                        row[idx] = 1
            mat.append(row)

        url = "http://localhost:4444/fca"
        headers = {'content-type': "application/json"}
        data = {"data": mat}
        try:
            response = requests.post(url, data=json.dumps(data, ensure_ascii=False), headers=headers)
        except ConnectionError as e:
            print "connection error"
            return all(v == 1 for v in [item for sublist in mat for item in sublist])
        resp_object = ujson.loads(response.text)
        resultFlag = False

        for obj in resp_object:
            if len(obj['properties']) > 0 and len(obj['molecules']) == 2 and 0 in obj['molecules']:
                resultFlag = True

        # if molecules[0]['head']['uri'] == "http://dbpedia.org/resource/Ben_Wyatt_d1_actor2" and molecules[1]['head']['uri'] == "http://dbpedia.org/resource/Ben_Wyatt_d2_actor1":
        #     print contextItems
        #     print mat
        #     print resp_object
        #     print resultFlag

        return resultFlag

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

    # def execute(self, rtl1, rtl2):
    #     self.left = rtl1
    #     self.right = rtl2
    #
    #     self.stage1(self.left, self.table2, self.table1,self.similarityMatrix, self.threshold, self.toBeJoined)
    #     self.stage1(self.right, self.table1, self.table2, self.similarityMatrix, self.threshold, self.toBeJoined)
    #
    # def stage1(self, rtl, other_table, own_table, similarity, threshold, output):
    #     # insert rtl1 into its own table
    #     if rtl not in own_table:
    #         own_table.append(rtl)
    #
    #     # probe rtl against the other table
    #     self.probe(rtl, other_table, similarity, threshold, output)
    #
    # def probe(self, rtl, table, similarity, threshold, output):
    #     probing_head = rtl['head']
    #     probing_head_index = probing_head['index']
    #     for record in table:
    #         head = record['head']
    #         head_index = head['index']
    #
    #         # either probing_head is row and head is column , or vice versa
    #
    #         if (probing_head, head) not in self.computedJoins:
    #             # check similarity using the threshold
    #             test_sim = self.sim(probing_head, head, similarity)
    #             if test_sim > threshold:
    #                 # (record, rtl) and (rtl, record) are considered the same in our case, check if it's already in the results
    #                 # if (record, rtl) not in output:
    #                 #     output.append((rtl, record))
    #                 self.computedJoins.append((probing_head, head))
    #                 if probing_head['row']:
    #                     self.result_matrix[(probing_head_index, head_index)] = test_sim
    #                 else:
    #                     self.result_matrix[(head_index, probing_head_index)] = test_sim
    #
    #             else:
    #                 self.computedJoins.append((probing_head, head))
    #                 if probing_head['row']:
    #                     self.result_matrix[(probing_head_index, head_index)] = 0
    #                 else:
    #                     self.result_matrix[(head_index, probing_head_index)] = 0
    #
    # def sim(self, incoming, existing, similarity):
    #     # if the incoming element is located in rows of the similarity matrix, then indexing will be [inc][exist]
    #     if incoming['row']:
    #         return similarity[incoming['index']][existing['index']]
    #     else:
    #         # otherwise the element is in columns, then indexing is [exist][inc]
    #         return similarity[existing['index']][incoming['index']]

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

        # produce the final result
        # find if rowIndex is in table1 or table2
        # if self.table1[0]['head']['row']:
        #     # index a is in table1
        #     for (a, b) in perfect_indexes:
        #         # find elements with indexes a,b in the left and right tables
        #
        #         self.toBeJoined.append(())
        # else:
        #     # index a is in table2
        #     for (a, b) in perfect_indexes
        #
        #

        return perfect_indexes
