import multiprocessing.managers
import multiprocessing
from multiprocessing.managers import SyncManager
import time
import sys
import LDA_Config
import json


class ServerQueueManager(SyncManager):
    pass


def run_client(host, port, authkey, number_of_cores):
    manager = make_client_manager(host, port, authkey)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    mp_work_allocator(job_q, result_q, number_of_cores)

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
        proc = multiprocessing.Process(
            target=LDA_worker,
            args=(shared_job_q, shared_result_q))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()


def send_beta_to_master(result_q, beta_object):
    result_not_put = True
    while result_not_put:
        if not result_q.full():
            result_q.put_nowait(beta_object)
            result_not_put = False
        else:
            result_not_put = True


def send_object_back_to_workers(job_q, job):
    object_not_put = True
    while object_not_put:
        if not job_q.full():
            job_q.put(job)
            object_not_put = False
        else:
            object_not_put = True


def LDA_worker(job_q, result_q):
    """ A worker function to be launched in a separate process. Takes jobs from
        job_q - each job a list of numbers to factorize. When the job is done,
        the result (dict mapping number -> list of factors) is placed into
        result_q. Runs until job_q is empty.
    """

    process_name = multiprocessing.current_process().name
    corpus_list = []
    corpus_received = False
    while True:
        if not job_q.full():
            job = job_q.get_nowait()
            job_name = job[0]
            if job_name.endswith("result"):
                initial_iteration = 0
                corpus_received = True
                corpus_name = job_name
                lda_object = job[1]
                print process_name, "Received corpus", corpus_name
                pre_LDA = time.time()
                lda_object.run_vb(verbose=False)
                print process_name, "finished running LDA on", corpus_name, \
                    "in", time.time() - pre_LDA, " seconds"

                if corpus_name.startswith("crashed_"):
                    corpus_name = corpus_name[8:]
                    server_iteration = job[2]
                    initial_iteration = server_iteration
                    lda_object.beta = job[3]

                corpus_list.append([corpus_name, lda_object, initial_iteration])
                beta_object = [corpus_name, lda_object.beta_matrix]
                send_beta_to_master(result_q, beta_object)
                print process_name, "sent back first beta of", corpus_name, initial_iteration

            elif job_name.startswith("Finished"):
                for current_corpus in corpus_list:
                    corpus_name = current_corpus[0]
                    lda_object = current_corpus[1]
                    result_q.put([corpus_name, lda_object.make_dictionary()])
                    print corpus_name, " finished"
                return

            elif corpus_received and job_name.startswith("beta"):

                for corpus in range(len(corpus_list)-1):
                    print corpus_list[corpus][0] + " EXTRA BETA"
                    result_not_gotten = True
                    while result_not_gotten:
                        try:
                            job = job_q.get_nowait()
                            result_not_gotten = False
                        except multiprocessing.Manager().Queue.Empty:
                            result_not_gotten = True


                new_beta = job[1]
                server_iteration = job[2]

                for corpus in corpus_list:
                    too_fast_count = 0
                    corpus_name = corpus[0]
                    lda_object = corpus[1]
                    current_iteration = corpus[2]
                    lda_object.beta_matrix = new_beta
                    time.sleep(2)


                    # if processor is ahead of others then put back job and wait 5 seconds
                    if current_iteration > server_iteration:
                        too_fast_count += 1
                        if too_fast_count > 2:
                            print "Extra Beta floating due to client crash"
                        else:
                            send_object_back_to_workers(result_q, job)
                            time.sleep(5)
                            print corpus_name, " too fast"
                    else:
                        pre_LDA = time.time()
                        lda_object.run_vb(initialise=False, verbose=False)
                        print process_name, "finished running LDA on", corpus_name, \
                            "in", time.time() - pre_LDA, " seconds"
                        beta_object = [corpus_name, lda_object.beta_matrix]
                        send_beta_to_master(result_q, beta_object)
                        print corpus_name, current_iteration, server_iteration
                        current_iteration += 1
                        corpus[2] = current_iteration

            else:
                print "Error " + str(job_name)
                return

        else:
            if not corpus_received:
                print process_name, "disconnected"
                return
            else:
                time.sleep(0.01)

    return


if __name__ == '__main__':
    number_of_cores = multiprocessing.cpu_count()
    connection_data = LDA_Config.get_connection_data()
    host = connection_data.host
    port = connection_data.port
    authkey = connection_data.authkey
    if len(sys.argv) == 2:
        number_of_cores = int(sys.argv[1])
    run_client(host, port, authkey, number_of_cores)
