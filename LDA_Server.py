import socket
from multiprocessing import Queue
from multiprocessing.managers import SyncManager
import time
import pickle
from functools import partial
from Queue import Queue as _Queue
import numpy as np
import sys

from LDA_Object import ldaObject
#from LDA_generate_corpus import get_corpus_list, get_alpha_list


corpus_list = pickle.load(open("corpus.p","rb"))
alpha_list = pickle.load(open("alpha.p","rb"))
initial_beta = pickle.load(open("initial_beta.p","rb"))
number_of_topics, vocabulary_size = initial_beta.shape
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
    # Start a shared manager server and access its queues
    manager = make_server_manager(port, "test")
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()
    processor_names = []
    count = 0

    while len(processor_names) < len(corpus_list):

        given_name = shared_result_q.get()
        new_name = "processor "+ str(count)
        print new_name
        processor_names.append(new_name)
        count += 1

    # get rid of extra processors
    time.sleep(1)
    while True:
        try:
            extra_processor = shared_result_q.get_nowait()
            shared_job_q.put(["Disconnect"])
        except:
            break
    time.sleep(1)

    print "Got All needed processes: ",processor_names
    start_time = time.time()
    for i in range(len(corpus_list)):
        lda_object = ldaObject([[]], corpus_list[i], alpha_list[i])
        shared_job_q.put([processor_names[i],lda_object])

    new_beta = np.zeros((number_of_topics, vocabulary_size))
    for it in range(100):
        processor_names_received =[]
        beta_sum = np.zeros((number_of_topics, vocabulary_size))
        start_waiting_time = time.time()
        while set(processor_names) != set(processor_names_received):
            result = shared_result_q.get()
            name = result[0]
            if name in processor_names_received:
                shared_job_q.put([new_beta])
                print name
            else:
                processor_names_received.append(name)
            beta = result[1]
            beta_sum += beta

        old_beta = new_beta
        row_sums = beta_sum.sum(axis=1)
        new_beta = beta_sum / row_sums[:, np.newaxis]
        beta_diff = np.sum(abs(np.subtract(new_beta, old_beta)))
        for j in range(len(corpus_list)):
            shared_job_q.put([new_beta])
        print "iteration: ", it, "beta difference: ", \
            beta_diff

    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.

    for j in range(len(corpus_list)):
        shared_job_q.put(["Finished"])

    end_time = time.time()
    print end_time - start_time, " seconds"
    time.sleep(2)

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
