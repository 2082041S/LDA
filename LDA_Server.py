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

def load_corpus_list(directory_path):
    return pickle.load(open("corpus.p", "rb"))


def construct_vocabulary(corpus_dict):
    vocabulary_dict = {}
    for corpus_name in corpus_dict:
        corpus = corpus_dict[corpus_name]
        for document in corpus:
            for word in corpus[document]:
                vocabulary_dict[word] = True
    return vocabulary_dict.keys()


def reduce_vocabulary_if_needed(corpus_dict, vocabulary):
    max_vocabulary_length = 10 ** 5
    document_appearance_min_threshold = 10
    init_length = len(vocabulary)
    # if there are too many words in the vocabulary the system must remove words that appear infrequently in documents
    if len(vocabulary) > max_vocabulary_length:
        new_vocabulary_dict = {}
        for corpus_name in corpus_dict:
            corpus = corpus_dict[corpus_name]
            word_document_count={}
            for document in corpus:
                for word in corpus[document]:
                    if word not in word_document_count:
                        word_document_count[word] = {}
                    if document not in word_document_count[word]:
                        word_document_count[word][document] = True

            for word in word_document_count:
                if len(word_document_count[word]) > document_appearance_min_threshold:
                    new_vocabulary_dict[word] = True

        for corpus_name in corpus_dict:
            corpus = corpus_dict[corpus_name]
            for document in corpus:
                new_doc = {}
                for word in corpus[document]:
                    if word in new_vocabulary_dict:
                        new_doc[word] = corpus[document][word]
                corpus[document] = new_doc
        print "Reduced vocabulary size from ",init_length," to ", len(vocabulary)
    return corpus_dict,vocabulary


def calculate_word_index(vocabulary):
    word_index = {}
    for pos in range (len(vocabulary)):
        word_index[vocabulary[pos]] = pos
    return word_index


def send_names_to_slaves(shared_job_q, corpus_dict, number_of_topics, word_index):
    corpus_names = []
    for name in corpus_dict:
        corpus_name = name + "_LDA_result"
        corpus_names.append(corpus_name)
        lda_object = VariationalLDA(corpus_dict[name], K=number_of_topics, eta=0.1, alpha=1, word_index=word_index)
        shared_job_q.put([corpus_name, lda_object])
    return corpus_names


def collect_betas_from_slaves(shared_result_q, shared_job_q, corpus_dict, number_of_topics, word_index,
                              corpus_names, beta_sum, new_beta, it, waiting_time_until_crash_assumed):
    begin_iteration_time = time.time()
    corpus_response_times = dict.fromkeys(corpus_names, 0)
    processor_names_received = []
    while set(corpus_names) != set(processor_names_received):
        try:
            result = shared_result_q.get_nowait()
            name = result[0]
            # print "Received result", name
            if name in processor_names_received:
                shared_job_q.put_nowait(["beta", new_beta, it])
                # print name
            else:
                processor_names_received.append(name)
                corpus_response_times[name] = time.time()

            beta = result[1]
            beta_sum += beta
            # print processor_names_received
        except:

            if it > 0 and (time.time() - begin_iteration_time > waiting_time_until_crash_assumed):
                begin_iteration_time = time.time()
                waiting_time_until_crash_assumed += 60
                for corpus_name in corpus_names:
                    if corpus_response_times[corpus_name] == 0:
                        name = corpus_name[:-11]
                        print "Client handling corpus ", corpus_name, " crashed", it
                        lda_object = VariationalLDA(corpus_dict[name], number_of_topics, 0.1, 1,
                                                    word_index=word_index)
                        lda_object.beta = new_beta
                        # shared_job_q.put(["crashed_" +corpus_name, lda_object, it-1])
                time.sleep(10)
                pass

    return beta_sum

def normalize_beta(beta_sum):
    row_sums = beta_sum.sum(axis=1)
    new_beta = beta_sum / row_sums[:, np.newaxis]
    return new_beta


def send_new_betas_to_slaves(shared_job_q, corpus_dict, new_beta, it):
    for j in range(len(corpus_dict)):
        result_not_put = True
        while result_not_put:
            try:
                shared_job_q.put_nowait(["beta", new_beta, it])
                result_not_put = False
            except:
                result_not_put = True


def signal_slaves_to_finish(shared_job_q, corpus_dict):
    for j in range(len(corpus_dict)):
        shared_job_q.put(["Finished"])


def collect_output_files_from_slaves(shared_result_q, shared_job_q, corpus_names):
    processor_names_received = []
    files = {}
    while set(corpus_names) != set(processor_names_received):
        result = shared_result_q.get()
        name = result[0]
        if name in processor_names_received:
            shared_job_q.put(["Finished"])
            print name
        else:
            print name
            print result[1]
            processor_names_received.append(name)
            # print processor_names_received
            files[result[1]] = result[2]
    return files


def create_output_files(files):
    cwd = os.getcwd()
    print "current directory " + cwd
    directory = cwd + "/results/"
    if not os.path.exists(directory):
        os.makedirs(directory)
    corpus_dict = {}
    corpus_dict["individual_lda"]= files.keys()
    pickle.dump(corpus_dict, open(directory +"corpus_dict.p", "wb"))
    for file_name in files:
        pickle.dump(files[file_name], open(directory + file_name + ".dict", "wb"))


class Master():
    def __init__(self, port, directory_path, file_name):
        self.corpus_dict = load_corpus_list(directory_path)
        print "Loaded input files"
        self.vocabulary = construct_vocabulary(self.corpus_dict)
        print "Constructed vocabulary of size", len(self.vocabulary)
        self.corpus_dict, self.vocabulary = reduce_vocabulary_if_needed(self.corpus_dict, self.vocabulary)
        self.word_index = calculate_word_index(self.vocabulary)
        self.number_of_topics = 500
        self.manager = make_server_manager(port, authkey="test")
        self.shared_job_q = self.manager.get_job_q()
        self.shared_result_q = self.manager.get_result_q()

    def runserver(self):

        start_time = time.time()
        corpus_names = send_names_to_slaves(self.shared_job_q,self.corpus_dict, self.number_of_topics, self.word_index)
        print corpus_names

        waiting_time_until_crash_assumed = 300
        new_beta = np.zeros((self.number_of_topics, len(self.vocabulary)))
        it = 0
        convergence_number = 0.01
        beta_diff = self.number_of_topics
        while beta_diff > convergence_number:
            beta_sum = np.zeros((self.number_of_topics, self.vocabulary_size))
            beta_sum = collect_betas_from_slaves(self.shared_result_q, self.shared_job_q, self.corpus_dict,
                                                 self.number_of_topics, self.word_index, corpus_names,
                                                 beta_sum, new_beta, it, waiting_time_until_crash_assumed)
            old_beta = new_beta
            new_beta = normalize_beta(beta_sum)
            beta_diff = np.sum(abs(np.subtract(new_beta, old_beta)))


            if beta_diff > convergence_number:
                send_new_betas_to_slaves(self.shared_job_q, self.corpus_dict, new_beta, it)

            print "iteration: ", it, "beta difference: ", beta_diff
            it += 1


        print "Finished converging Beta " + str(beta_diff)
        signal_slaves_to_finish(self.shared_job_q, self.corpus_dict)
        files = collect_output_files_from_slaves(self.shared_result_q, self.shared_job_q, corpus_names)
        print "Got back all files " + str(len(files))
        create_output_files(files)


        end_time = time.time()
        print end_time - start_time, " seconds"
        time.sleep(5)

        self.manager.shutdown()

if __name__ == '__main__':
    port = 8765
    if len(sys.argv) == 2:
        port = int(sys.argv[1])
    master = Master(port,"","")
    master.runserver()
