import multiprocessing.managers
import multiprocessing
from multiprocessing.managers import SyncManager
import time
import sys
import LDA_Config


# Setting up the distributed system is inspired from:
# http://eli.thegreenplace.net/2012/01/24/distributed-computing-in-python-with-multiprocessing

# SyncManager is subclass of BaseManager which can be used
# for the synchronization of processes.
# ServerQueueManager will expose the get_job_q and get_result_q
# methods for accessing the shared queues from the server
# This class is at top level so that Queue can be pickled.
class ServerQueueManager(SyncManager):
    pass


# Accesses server through manager object and gets the
# queues. It then allocations work for every CPU the
# computer is asked to use
def run_client(host, port, authkey, number_of_cores, e_step_its):
    manager = make_client_manager(host, port, authkey)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    server_iteration_q = manager.get_server_iteration_q()
    client_crash_q = manager.get_client_crash_q()
    mp_work_allocator(job_q, result_q, server_iteration_q, client_crash_q, number_of_cores, e_step_its)


# Create a manager for a client. This manager connects to a server on the
# given address and exposes the get_job_q and get_result_q methods for
# accessing the shared queues from the server
def make_client_manager(host, port, authkey):
    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')
    ServerQueueManager.register('get_server_iteration_q')
    ServerQueueManager.register('get_client_crash_q')
    manager = ServerQueueManager(address=(host, port), authkey=authkey)
    manager.connect()

    print 'Client connected to %s:%s' % (host, port)
    return manager


# Split the work with jobs in shared_job_q and results in
# shared_result_q into several processes. Launch each process with
# LDA_worker as the worker function, and wait until all are finished.
def mp_work_allocator(shared_job_q, shared_result_q, server_iteration_q, client_crash_q, nprocs, e_step_its):
    procs = []
    for _ in range(nprocs):
        proc = multiprocessing.Process(
            target=LDA_worker,
            args=(shared_job_q, shared_result_q, server_iteration_q, client_crash_q, e_step_its))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()


def send_object_to_master(result_q, object):
    result_not_put = True
    while result_not_put:
        if not result_q.full():
            result_q.put(object)
            result_not_put = False
        else:
            result_not_put = True


def send_object_back_to_workers(job_q, job):
    object_not_put = True
    while object_not_put:
        if not job_q.full():
            job_q.put(job)
            #print "Sent Back"
            object_not_put = False
        else:
            object_not_put = True


# gets beta from server. Since all betas
# sent by server are the same there is no need
# for worker to store the extra betas
def get_extra_beta_from_server(job_q):
    result_not_gotten = True
    while result_not_gotten:
        if not job_q.empty():
            # since the server sends the same beta
            # there is no need to store it
            extra_beta = job_q.get()
            result_not_gotten = False
        else:
            result_not_gotten = True


def get_beta_for_each_extra_corpus(job_q, corpus_list):
    for corpus in range(len(corpus_list) - 1):
        # print corpus_list[corpus][0] + " EXTRA BETA"
        get_extra_beta_from_server(job_q)


def handle_worker_ahead_scenario(job_q, job, corpus_name, too_fast_count, average_LDA_time):
    too_fast_count += 1
    # if the worker gets ahead of iteration twice then assume crash
    if too_fast_count > 3:
        print "Extra Beta floating due to client crash"
    else:
        print corpus_name, " too fast"
        send_object_back_to_workers(job_q, job)
        time.sleep(average_LDA_time)
    return too_fast_count


def signal_all_corpuses_to_send_results(result_q, corpus_list):
    for current_corpus in corpus_list:
        corpus_name = current_corpus[0]
        lda_object = current_corpus[1]
        # lda_object.make_dictionary() is the result of the corpus
        corpus_results_object = [corpus_name, lda_object.make_dictionary()]
        send_object_to_master(result_q, corpus_results_object)
        print corpus_name, " finished"


def update_corpus_betas_and_send_new_betas_to_master(result_q, job_q, job, 
                                                     corpus_list, new_beta, e_step_its):
    for corpus in corpus_list:
        corpus_name = corpus[0]
        lda_object = corpus[1]
        lda_object.beta_matrix = new_beta

        pre_LDA = time.time()
        #print process_name, corpus_name
        lda_object.run_vb(initialise=False, e_step_its= e_step_its, verbose=False)
        LDA_execution_time = time.time() - pre_LDA
        print "Finished running LDA on", corpus_name, \
           "in", time.time() - pre_LDA, " seconds"

        beta_object = [corpus_name, lda_object.beta_matrix, LDA_execution_time]
        send_object_to_master(result_q, beta_object)

    return corpus_list

def update_corpus_list_and_send_new_beta_to_master(result_q, process_name, corpus_list ,
                                                   lda_object, corpus_name, e_step_its):
    pre_LDA = time.time()
    lda_object.run_vb(verbose=False, e_step_its= e_step_its, initialise=True)
    LDA_execution_time = time.time() - pre_LDA
    print process_name, "finished running LDA on", corpus_name, \
       "in", LDA_execution_time, " seconds"

    corpus_list.append([corpus_name, lda_object])
    beta_object = [corpus_name, lda_object.beta_matrix, LDA_execution_time]
    send_object_to_master(result_q, beta_object)
    #print process_name, "sent back first beta of", corpus_name, current_iteration
    return corpus_list

# in case of client crash the server sent:
# ["corpus", "crashed_" + corpus_name, lda_object,  new_beta]
# Otherwise, server sent ["corpus", corpus_name, lda_object]
def get_corpus_from_master(job, server_iteration_q):
    lda_object = job[2]
    if job[1].startswith("crashed_"):
        # remove "crashed_" from corpus_name
        corpus_name = job[1][8:]
        lda_object.beta = job[3]
    else:
        corpus_name = job[1]

    return corpus_name, lda_object


def LDA_worker(job_q, result_q, server_iteration_q, client_crash_q, e_step_its):
    process_name = multiprocessing.current_process().name
    # list of corpuses that the processor handles
    corpus_list = []
    corpus_received = False
    process_current_iteration = 0
    while True:
        client_crashed = client_crash_q.qsize() == 1
        # if number_of_corpora == job_q.qsize():
        #     print number_of_corpora
        #     all_work_sent_by_master = True 
            
        # if (all_work_sent_by_master or first_iteration) and job_q.qsize() == 0:
        #     all_work_sent_by_master = False
        #     first_iteration = False

        if process_current_iteration <= server_iteration_q.qsize() or client_crashed:
            
            try:
                job = job_q.get_nowait()
                #print process_current_iteration, server_iteration_q.qsize()
                # job_name indicates the object the server sent
                # i.e. a corpus; beta or "finished"(signal to finish)
                job_name = job[0]

                if job_name.startswith("corpus"):
                    corpus_received = True
                    #print "Received", job[1], job_q.qsize()
                    corpus_name, lda_object = get_corpus_from_master(job, server_iteration_q)
                    #print process_name, "Received corpus", corpus_name
                    corpus_list = update_corpus_list_and_send_new_beta_to_master(result_q, process_name, 
                                                        corpus_list ,lda_object, corpus_name, e_step_its)

                # must ensure that client has corpus to work on
                # before computing new_betas
                elif job_name.startswith("beta"):
                    if corpus_received:
                        print "Iteration", process_current_iteration
                        new_beta = job[1]
                        get_beta_for_each_extra_corpus(job_q, corpus_list)
                        corpus_list = update_corpus_betas_and_send_new_betas_to_master(result_q, job_q,  
                                                                            job, corpus_list, new_beta, e_step_its)
                        process_current_iteration +=1
                    else: 
                        print process_name, "lacks a corpus and hence disconnects"
                        send_object_back_to_workers(job_q, job)

                elif job_name.startswith("Finished"):
                    signal_all_corpuses_to_send_results(result_q, corpus_list)
                    return

                # server did not send corpus,beta or "Finished" object
                # This is unexepected job received and the client will
                # terminate as faulty behaviour is discovered
                else:
                    print "Error " + process_name + " " + str(job_name) 
                    return

            except:
                # if the client does did not receive any corpus
                # there is no work for him to do and he disconnects
                if not corpus_received:
                    print process_name, "disconnected"
                    return
                else:
                    time.sleep(0.01)

    return


# gets input data from configuration file
# or command prompt
def get_input_data():
    # default value for number_of_cores is the
    # number of CPU the computer can handle
    number_of_cores = multiprocessing.cpu_count()
    client_config_data = LDA_Config.get_client_data()
    host = client_config_data.host
    port = client_config_data.port
    authkey = client_config_data.authkey
    e_step_its = client_config_data.e_step_its
    if len(sys.argv) == 2:
        number_of_cores = int(sys.argv[1])

    return host, port, authkey, number_of_cores, e_step_its


if __name__ == '__main__':
    host, port, authkey, number_of_cores, e_step_its = get_input_data()
    run_client(host, port, authkey, number_of_cores, e_step_its)
