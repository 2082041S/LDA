from multiprocessing import Queue
import multiprocessing.managers
import multiprocessing
from multiprocessing.managers import SyncManager
import time
from Queue import Queue as _Queue

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
    mp_work_allocator(job_q, result_q, 4)


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
    process_iteration = -1
    process_name = multiprocessing.current_process().name
    multiprocessing.cpu_count()
    server_iteration = 0
    while True:
            process_iteration += 1
            if process_iteration == 0:
                result_q.put(process_name)
            else:
                job = job_q.get()
                if job[0] == "Finished":
                    return
                if process_iteration == 1:
                    process_name = job[0]
                    lda_object = job[1]
                    lda_object.run_LDA()
                    result_q.put([process_name, lda_object.beta])
                else:
                    lda_object.update_beta(job[0])
                    lda_object.run_LDA()
                    result_q.put([process_name, lda_object.beta])
                print process_name, process_iteration
    return

if __name__ == '__main__':
    if len(sys.argv) == 3:
        host = sys.argv[1]
        port = int(sys.argv[2])
        runclient(host, port)
    else:
        print "Please provide <host> and <port>"
