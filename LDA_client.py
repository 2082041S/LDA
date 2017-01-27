import multiprocessing.managers
import multiprocessing
from multiprocessing.managers import SyncManager
import time
import numpy as np
from multiprocessing import Manager
import sys




class ServerQueueManager(SyncManager):
    pass

# class Beta_Holder():
#     def __init__(self, beta=[]):
#         self.beta = beta

def runclient(host,port, number_of_cores):
    manager = make_client_manager(host, port, "test")
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
                       
                    lda_object = job[1]
                    print process_name, "Received corpus", corpus_name  
                    pre_LDA = time.time()
                    lda_object.run_vb(verbose = False)
                    print process_name, "finished running LDA on", corpus_name, "in", time.time() - pre_LDA," seconds"
                    if corpus_name.startswith("crashed_"):
                        corpus_name= corpus_name[8:]
                        server_iteration = job[2]
                        initial_iteration = server_iteration
                        lda_object.beta = job[3]                    

                    
                    corpus_list.append([corpus_name, lda_object, initial_iteration])    
                    result_not_put = True
                    # beta_holder = Beta_Holder(lda_object.beta_matrix)

                    while result_not_put:
                        try:
                            result_q.put_nowait([corpus_name, lda_object.beta_matrix])
                            result_not_put = False
                        except:
                            result_not_put = True          
                    print process_name, "sent back first beta of", corpus_name, initial_iteration

                elif job[0].startswith("Finished"):
                    for current_corpus in corpus_list:
                        corpus_name = current_corpus[0]
                        lda_object = current_corpus[1]
                        result_q.put([corpus_name, lda_object.make_dictionary()])
                        print corpus_name, " finished"                  
                    return

                elif corpus_received and job[0].startswith("beta"):

                    for corpus in range(len(corpus_list)-1):
                            print corpus_list[corpus][0] + " EXTRA BETA"
                            result_not_gotten = True
                            while result_not_gotten:
                                try:
                                    job = job_q.get_nowait()
                                    result_not_gotten = False
                                except:
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
                            too_fast_count +=1
                            if too_fast_count > 2:
                                print "Extra Beta floating due to client crash"
                            else:
                                result_not_put = True
                                while result_not_put:
                                    try:
                                        job = job_q.put(job) 
                                        result_not_put = False
                                    except:
                                        result_not_put = True
                                                      
                                time.sleep(5)
                                print corpus_name," too fast"
                        else:
                            pre_LDA = time.time()
                            lda_object.run_vb(initialise=False, verbose = False)
                            print process_name, "finished running LDA on", corpus_name, "in", time.time() - pre_LDA," seconds"
                            # current_iteration += 1
                            corpus[2] = current_iteration
                            # beta_holder = Beta_Holder(lda_object.beta_matrix)
                            # print "Created Beta_Holder"
                            result_not_put = True
                            while result_not_put:
                                try:
                                    result_q.put([corpus_name, lda_object.beta_matrix])
                                    result_not_put = False
                                    print corpus_name, current_iteration ,server_iteration
                                    current_iteration += 1
                                    corpus[2] = current_iteration
                                except:
                                    result_not_put = True   

                else:
                    print "Error " + str(job[0])
                    return

            except:
                if not corpus_received:
                    print process_name, "disconnected"
                    return
                else:
                    time.sleep(0.01)

    return

if __name__ == '__main__':
    if len(sys.argv) == 4:
        host = sys.argv[1]
        port = int(sys.argv[2])
        number_of_cores = int(sys.argv[3])
        runclient(host, port, number_of_cores)
    else:
        print "Please provide <host> and <port> and <number_of_cores>"
