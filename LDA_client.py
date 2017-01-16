from multiprocessing import Queue
import multiprocessing.managers
import multiprocessing
from multiprocessing.managers import SyncManager
import time
from Queue import Queue as _Queue
import numpy as np

import sys


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

class ServerQueueManager(SyncManager):
    pass


def runclient(host,port):
    manager = make_client_manager(host, port, "test")
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    mp_work_allocator(job_q, result_q, multiprocessing.cpu_count())


def make_client_manager(host, port, authkey):
    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')
    manager = ServerQueueManager(address=(host, port), authkey=authkey)
    manager.connect()

    print 'Client connected to %s:%s' % (host, port)
    return manager

def mp_work_allocator(shared_job_q, shared_result_q, nprocs):
    """ Split the work with jobs in shared_job_q and results in
        shared_result_q into several processes. Launch each process with
        LDA_worker as the worker function, and wait until all are
        finished.
    """

    procs = []
    for i in range(nprocs):
        p = multiprocessing.Process(
                target=LDA_worker,
                args=(shared_job_q, shared_result_q))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()

def LDA_worker(job_q, result_q):
    """ A worker function to be launched in a separate process. Takes jobs from
        job_q - each job a list of numbers to factorize. When the job is done,
        the result (dict mapping number -> list of factors) is placed into
        result_q. Runs until job_q is empty.
    """

    lda_object =[]
    process_name = multiprocessing.current_process().name
    corpus_list = []
    current_lda_object_index = 0
    server_iteration = 0
    corpus_count = 0
    corpus_received = False
    while True:
            try:
                job = job_q.get_nowait()

                if job[0].endswith("result"):   
                    initial_iteration = 0
                    corpus_received = True
                    corpus_name = job[0]
                    if corpus_name.startswith("crashed_"):
                        corpus_name= corpus_name[8:]
                        server_iteration = job[2]
                        current_iteration = server_iteration
                        initial_iteration = server_iteration

                    print process_name, "Received corpus", corpus_name
                    lda_object = job[1]
                    lda_object.run_vb()
                    corpus_list.append([corpus_name, lda_object, initial_iteration])
                    current_lda_object_index = len(corpus_list) - 1                  
                    result_q.put([corpus_name,lda_object.beta_matrix])
                    print process_name, "sent back first beta of", corpus_name, initial_iteration

                elif job[0].startswith("Finished"):
                    current_lda_object_index  = (current_lda_object_index + 1) % len(corpus_list)
                    current_corpus = corpus_list[current_lda_object_index]
                    corpus_name = current_corpus[0]
                    lda_object = current_corpus[1]
                    filename = corpus_name
                    result_not_put = True
                    result_q.put([corpus_name, filename, lda_object.make_dictionary()])
                    print corpus_name, " finished"                  
                    corpus_count += 1
                    if corpus_count >= len(corpus_list):
                        return

                elif corpus_received and job[0].startswith("beta"):
                    new_beta = job[1]
                    current_lda_object_index  = (current_lda_object_index + 1) % len(corpus_list)
                    current_corpus = corpus_list[current_lda_object_index]
                    current_iteration = current_corpus[2]
                    corpus_name = current_corpus[0]
                    lda_object = current_corpus[1]
                    lda_object.beta_matrix = new_beta
                    server_iteration = job[2]
                    print corpus_name, current_iteration ,server_iteration

                    # if processor is ahead of others then put back job and wait 3 seconds
                    if current_iteration > server_iteration:
                        too_fast_count +=1
                        if too_fast_count > 1:
                            print "Extra Beta floating due to client crash"
                        else:
                            job_q.put(job)                       
                            time.sleep(5)
                            print corpus_name," too fast"

                    else:
                        lda_object.run_vb(initialise=False)
                        current_corpus[2] += 1
                        result_not_put = True
                        if np.isnan(lda_object.beta_matrix[0][0]):
                            print "Corpus name",corpus_name
                            print "First element of beta matrix", lda_object.beta_matrix[0][0]
                        result_q.put([corpus_name, lda_object.beta_matrix])
                        too_fast_count = 0

                else:
                    print "Error " + str(job[0])
                    return

            except:
                if not corpus_received:
                    print process_name, "disconnected"
                    return
                else:
                    pass

    return

if __name__ == '__main__':
    if len(sys.argv) == 3:
        host = sys.argv[1]
        port = int(sys.argv[2])
        runclient(host, port)
    else:
        print "Please provide <host> and <port>"
