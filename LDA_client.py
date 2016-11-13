from multiprocessing import Queue
import multiprocessing.managers
import multiprocessing
from multiprocessing.managers import SyncManager
import time
from Queue import Queue as _Queue


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


def runclient():
    manager = make_client_manager("192.168.56.1", 12345, "test")
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    mp_work_allocator(job_q, result_q, 4)

def make_client_manager(IP, port, authkey):
    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')
    manager = ServerQueueManager(address=(IP, port), authkey=authkey)
    manager.connect()

    print 'Client connected to %s:%s' % (IP, port)
    return manager

def mp_work_allocator(shared_job_q, shared_result_q, nprocs):
    """ Split the work with jobs in shared_job_q and results in
        shared_result_q into several processes. Launch each process with
        LDA_worker as the worker function, and wait until all are
        finished.
    """
    start_time = time.time()
    procs = []
    for i in range(nprocs):
        p = multiprocessing.Process(
                target=LDA_worker,
                args=(shared_job_q, shared_result_q))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()

    end_time = time.time()
    print end_time - start_time," seconds"


def LDA_worker(job_q, result_q):
    """ A worker function to be launched in a separate process. Takes jobs from
        job_q - each job a list of numbers to factorize. When the job is done,
        the result (dict mapping number -> list of factors) is placed into
        result_q. Runs until job_q is empty.
    """
    empty = False
    lda_object =[]
    lda_list =[]
    counter = 0
    first_beta = True
    while True:
        try:
            job = job_q.get_nowait()
            print multiprocessing.current_process()
            if job[0] == "corpus":
                time.sleep(0.01)
                lda_object = job[1]
                lda_list.append(lda_object)
                lda_object.run_LDA()
            elif job[0] == "beta":
                if first_beta:
                    time.sleep(0.01)
                    first_beta = False
                lda_object = lda_list[counter]
                counter = (counter + 1) % len(lda_list)
                lda_object.update_beta(job[1])
                lda_object.run_LDA()
            elif job[0] == "wait":
                pass
            else:
                print "ERROR: Unexpected job"
                return;
            result_q.put(lda_object.beta)
        except:

            return

    return

if __name__ == '__main__':
    runclient()