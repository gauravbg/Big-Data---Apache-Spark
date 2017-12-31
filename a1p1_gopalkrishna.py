########################################
## Template Code for Big Data Analytics
## assignment 1 - part I, at Stony Brook Univeristy
## Fall 2017


import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
import math
from random import *



##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:  # [TODO]
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3):  # [DONE]
        self.data = data  # the "file": list of all key value pairs
        self.num_map_tasks = num_map_tasks  # how many processes to spawn as map tasks
        self.num_reduce_tasks = num_reduce_tasks  # " " " as reduce tasks

    ###########################################################
    # programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v):  # [DONE]
        print
        "Need to override map"

    @abstractmethod
    def reduce(self, k, vs):  # [DONE]
        print
        "Need to override reduce"

    ###########################################################
    # System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r):  # [DONE]
        # runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            # run mappers:
            mapped_kvs = self.map(k, v)
            # assign each kv pair to a reducer task
            for (k, v) in mapped_kvs:
                namenode_m2r.append((self.partitionFunction(k), (k, v)))

    def partitionFunction(self, k):  # [TODO]
        hash = 7;
        for c in str(k):
            hash = hash * 13 + ord(c)
        return hash % self.num_reduce_tasks

    # given a key returns the reduce task to send it


    def reduceTask(self, kvs, namenode_fromR):  # [TODO]
        reduce_dict = dict()
        for (k,v) in kvs:
            reduce_dict.setdefault(k, []).append(v)
        for k, vs in reduce_dict.items():
            namenode_fromR.append(self.reduce(k, vs))
    # sort all values for each key (can use a list of dictionary)


    # call reducers on each key with a list of values
    # and append the result for each key to reduced_tuples
    # [TODO]


    def runSystem(self):  # [TODO]
        # runs the full map-reduce system processes on mrObject

        # the following two lists are shared by all processes
        # in order to simulate the communication
        # [DONE]
        namenode_m2r = Manager().list()  # stores the reducer task assignment and
        # each key-value pair returned from mappers
        # in the form: [(reduce_task_num, (k, v)), ...]
        namenode_fromR = Manager().list()  # stores key-value pairs returned from reducers
        # in the form [(k, v), ...]

        # divide up the data into chunks accord to num_map_tasks, launch a new process
        # for each map task, passing the chunk of data to it.
        # hint: if chunk contains the data going to a given maptask then the following
        #      starts a process

        start = 0
        inc = math.ceil(len(self.data)/self.num_map_tasks)
        map_jobs = list()
        for mapper_index in range(self.num_map_tasks):
            end = (start+inc) if ((start+inc)<len(self.data)) else len(self.data)
            if start < len(self.data):
                chunk = self.data[start:end]
                p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
                map_jobs.append(p)
                p.start()
                start += inc
                #  (it might be useful to keep the processes in a list)
                # [TODO]


        # join map task processes back
        # [TODO]
        for job in map_jobs:
            job.join()


        # print output from map tasks
        # [DONE]
        print("namenode_m2r after map tasks complete:")
        map_out = sorted(list(namenode_m2r))
        pprint(map_out)

        # "send" each key-value pair to its assigned reducer by placing each
        # into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)]
        for item in map_out:
            to_reduce_task[item[0]].append(item[1])
        # [TODO]


        # launch the reduce tasks as a new process for each.
        # [TODO]
        reduce_jobs = list()
        for reducer_index in range(self.num_reduce_tasks):
            kvs = to_reduce_task[reducer_index]
            p = Process(target=self.reduceTask, args=(kvs, namenode_fromR))
            reduce_jobs.append(p)
            p.start()

        # join the reduce tasks back
        # [TODO]
        for job in reduce_jobs:
            job.join()


        # print output from reducer tasks
        # [DONE]
        print("namenode_fromR after reduce tasks complete:")
        result = list(namenode_fromR)
        pprint(sorted([x for x in result if x is not None]))

        # return all key-value pairs:
        # [DONE]
        return namenode_fromR


##########################################################################
##########################################################################
##Map Reducers:

class WordCountMR(MyMapReduce):  # [DONE]
    # the mapper and reducer for word count
    def map(self, k, v):  # [DONE]
        counts = dict()
        for w in v.split():
            w = w.lower()  # makes this case-insensitive
            try:  # try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()

    def reduce(self, k, vs):  # [DONE]
        return (k, np.sum(vs))


class SetDifferenceMR(MyMapReduce):  # [TODO]
    # contains the map and reduce function for set difference
    # Assume that the mapper receives the "set" as a list of any primitives or comparable objects
    def map(self, k, v):  # [DONE]
        set_dict = dict()
        for item in v:
            set_dict[item] = k
        return set_dict.items()
    def reduce(self, k, vs):  # [DONE]
        if len(vs) == 1 and vs[0] == 'R':
            return str(k)




##########################################################################
##########################################################################


if __name__ == "__main__":  # [DONE: Uncomment peices to test]
    ###################
    ##run WordCount:
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8,
             "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")]
    mrObject = WordCountMR(data, 4, 3)
    mrObject.runSystem()

    ####################
    ##run SetDifference
    # (TODO: uncomment when ready to test)
    print("\n\n*****************\n Set Difference\n*****************\n")
    data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']), ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
    data2 = [('R', [x for x in range(50) if random() > 0.5]), ('S', [x for x in range(50) if random() > 0.75])]
    mrObject = SetDifferenceMR(data1, 4, 2)
    mrObject.runSystem()
    mrObject = SetDifferenceMR(data2, 4, 1)
    mrObject.runSystem()



