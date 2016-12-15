import socket
from multiprocessing import Queue
from multiprocessing.managers import SyncManager
import time
import cPickle as pickle
import os
from functools import partial
from Queue import Queue as _Queue
import numpy as np
import sys

from lda import VariationalLDA
from LDA_Object import ldaObject
#from LDA_generate_corpus import get_corpus_list, get_alpha_list


class Queue(_Queue):
    """ A picklable queue. """

    def __getstate__(self):
        # Only pickle the state we care about
        return (self.maxsize, self.queue, self.unfinished_tasks)

    def __setstate__(self, state):
        # Re-initialize the object, then overwrite the default state with
        # our pickled state.
        Queue.__init__(self)
        self.maxsize = state[0]
        self.queue = state[1]
        self.unfinished_tasks = state[2]


def get_q(q):
    return q


class JobQueueManager(SyncManager):
    pass


def runserver(port):
    print "Start"
    corpus_list = pickle.load(open("corpus.p", "rb"))
    print "Received corpus"
    vocabulary = pickle.load(open("vocabulary.p", "rb"))
    print "Received vocabulary"
    vocabulary_size = len(vocabulary)
    number_of_topics = 500
    # Start a shared manager server and access its queues
    manager = make_server_manager(port, "test")
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()
    corpus_names = []


    start_time = time.time()
    for i in range(len(corpus_list)):
        corpus_names.append("corpus" + str(i))
        lda_object = VariationalLDA(corpus_list[i], number_of_topics, 0.1, 1)
        # lda_object = ldaObject([[]], corpus_list[i], alpha_list[i])
        shared_job_q.put([corpus_names[i],lda_object])


    print corpus_names
    new_beta = np.zeros((number_of_topics, vocabulary_size))
    it =0
    beta_diff = number_of_topics

    while beta_diff >0.1:
        processor_names_received =[]
        beta_sum = np.zeros((number_of_topics, vocabulary_size))


        while set(corpus_names) != set(processor_names_received):
            result = shared_result_q.get()
            name = result[0]

            if name in processor_names_received:
                shared_job_q.put(["beta", new_beta, it])
                print name

            else:
                processor_names_received.append(name)
            beta = result[1]
            beta_sum += beta
            print processor_names_received
        old_beta = new_beta
        row_sums = beta_sum.sum(axis=1)
        new_beta = beta_sum / row_sums[:, np.newaxis]
        beta_diff = np.sum(abs(np.subtract(new_beta, old_beta)))
        if beta_diff > 0.1:
            for j in range(len(corpus_list)):
                shared_job_q.put(["beta", new_beta, it])
        print "iteration: ", it, "beta difference: ", beta_diff
        it += 1

    print "Finished converging Beta " + str(beta_diff)

    for j in range(len(corpus_list)):
        shared_job_q.put(["Finished"])

    processor_names_received =[]
    files = {}
    while set(corpus_names) != set(processor_names_received):
        result = shared_result_q.get()
        name = result[0]
        if name in processor_names_received:
            shared_job_q.put(["Finished"])
            print name
        else:
            processor_names_received.append(name)
            #print processor_names_received
            files[result[1]] = result[2]

    print "Got back all files " +str(len(files))



    cwd = os.getcwd()
    print "current directory " + cwd
    directory = cwd + "/results/"
    pickle.dump(files.keys(), open(directory +"corpus_list.p", "wb"))
    for file in files:
        if not os.path.exists(directory):
            os.makedirs(directory)
        pickle.dump(files[file], open(directory + file, "wb"))
    end_time = time.time()
    print end_time - start_time, " seconds"
    time.sleep(5)

    manager.shutdown()


def make_server_manager(port, authkey):
    job_q = Queue()
    result_q = Queue()

    JobQueueManager.register('get_job_q', callable=partial(get_q, job_q))
    JobQueueManager.register('get_result_q', callable=partial(get_q, result_q))

    manager = JobQueueManager(address=(socket.gethostname(), port), authkey=authkey)
    manager.start()
    print('Server hostname: %s' %socket.gethostname())
    print('Server started at port %s' % port)
    return manager

if __name__ == '__main__':
    port = 8765
    if len(sys.argv) == 2:
        port = int(sys.argv[1])
    runserver(port)
