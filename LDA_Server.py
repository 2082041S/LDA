import socket
from functools import partial
from Queue import Queue as _Queue
from multiprocessing.managers import SyncManager
import time
import cPickle as pickle
import os
import numpy as np
import sys
from lda import VariationalLDA
import pdb


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


class Master:
    def __init__(self, port, directory_paths):
        self.manager = self.make_server_manager(port, authkey="test")
        self.shared_job_q = self.manager.get_job_q()
        self.shared_result_q = self.manager.get_result_q()
        self.corpus_dict = self.load_corpus_dict(directory_paths)
        print "Loaded input files"
        self.vocabulary = self.construct_vocabulary()
        print "Constructed vocabulary of size", len(self.vocabulary)
        self.reduce_vocabulary_if_needed()
        self.word_index = self.calculate_word_index()
        self.number_of_topics = 500


    def make_server_manager(self, port, authkey):
        host = socket.gethostname()
        print host
        job_q = Queue()
        result_q = Queue()
        JobQueueManager.register('get_job_q', callable=partial(get_q, job_q))
        JobQueueManager.register('get_result_q', callable=partial(get_q, result_q))
        manager = JobQueueManager(address=(host, port), authkey=authkey)
        manager.start()
        print'Server hostname: %s' % host
        print'Server started at port %s' % port
        return manager


    def load_corpus_dict(self, directory_paths):
        corpus_dict = {}
        for directory in directory_paths:
            file_list = os.listdir(directory)
            # vocabulary = {}
            for file_name in file_list:
                if file_name.endswith(".dict"):
                    corpus = pickle.load(open(directory + "/" + file_name, "rb"))
                    corpus_dict[file_name[:-5]] = corpus
                    # for document in corpus:
                    #     for word in corpus[document]:
                    #         vocabulary[word] = True
                if file_name.endswith("corpus.p"):
                    corpus_list = pickle.load(open(directory + "/" + file_name, "rb"))
                    for corpus in corpus_list:
                        corpus_dict[corpus] = corpus_list[corpus]

        return corpus_dict


    def construct_vocabulary(self):
        vocabulary_dict = {}
        for corpus_name in self.corpus_dict:
            corpus = self.corpus_dict[corpus_name]
            for document in corpus:
                for word in corpus[document]:
                    vocabulary_dict[word] = True

        return vocabulary_dict.keys()


    def reduce_vocabulary_if_needed(self):
        max_vocabulary_length = 50000
        init_length = len(self.vocabulary)
        document_appearance_min_threshold = 10
        # if there are too many words in the vocabulary then
        # the system must remove words that appear infrequently in documents
        if len(self.vocabulary) > max_vocabulary_length:
            new_vocabulary_dict = {}
            for corpus_name in self.corpus_dict:
                corpus = self.corpus_dict[corpus_name]
                word_document_count = {}
                for document in corpus:
                    for word in corpus[document]:
                        if word not in word_document_count:
                            word_document_count[word] = {}
                        if document not in word_document_count[word]:
                            word_document_count[word][document] = True

                for word in word_document_count:
                    if len(word_document_count[word]) > document_appearance_min_threshold:
                        new_vocabulary_dict[word] = True

            for corpus_name in self.corpus_dict:
                corpus = self.corpus_dict[corpus_name]
                for document in corpus:
                    new_doc = {}
                    for word in corpus[document]:
                        if word in new_vocabulary_dict:
                            new_doc[word] = corpus[document][word]
                    corpus[document] = new_doc
            self.vocabulary = new_vocabulary_dict.keys()
            print "Reduced vocabulary size from ", init_length, " to ", len(self.vocabulary)


    def calculate_word_index(self):
        word_index = {}
        for pos in range(len(self.vocabulary)):
            word_index[self.vocabulary[pos]] = pos
        return word_index


    def send_object_to_workers(self, object):
        object_not_sent = True
        while object_not_sent:
            if not self.shared_job_q.full():
                self.shared_job_q.put(object)
                object_not_sent = False
            else:
                object_not_sent = True

                
    def get_object_from_workers(self,):
        object_not_received = True
        result = None
        while object_not_received:
            if not self.shared_result_q.empty():
                result = self.shared_result_q.get()
                object_not_received = False
            else:
                object_not_received = True
        return result

    def send_names_to_workers(self):
        corpus_names = []
        for name in self.corpus_dict:
            corpus_name = name + "_LDA_result"
            corpus_names.append(corpus_name)
            lda_object = VariationalLDA(self.corpus_dict[name], K=self.number_of_topics, eta=0.1, alpha=1,
                                        word_index=self.word_index, normalise=1000)
            self.shared_job_q.put([corpus_name, lda_object])
        return corpus_names


    def collect_betas_from_workers(self, corpus_names, beta_sum, new_beta, it, waiting_time_until_crash_assumed):

        count = 1
        begin_iteration_time = time.time()
        first_result_time = 0
        corpus_response_times = dict.fromkeys(corpus_names, 0)
        processor_names_received = []
        while set(corpus_names) != set(processor_names_received):
            try:
                result = self.shared_result_q.get_nowait()
                name = result[0]
                print "Received " + name + " " + str(count) + "/" + str(len(corpus_names))
                if name in processor_names_received:
                    self.shared_job_q.put_nowait(["beta", new_beta, it])
                    # print name
                else:
                    count += 1
                    if count == 1:
                        first_result_time = time.time()
                    processor_names_received.append(name)
                    corpus_response_times[name] = time.time()

                beta = result[1]
                beta_sum += beta
                # print processor_names_received
            except:

                if first_result_time == 0:
                    client_crashed_assumed = (time.time() - begin_iteration_time) > waiting_time_until_crash_assumed
                else:
                    client_crashed_assumed = (time.time() - first_result_time) > waiting_time_until_crash_assumed

                if client_crashed_assumed:
                    pdb.set_trace()
                    begin_iteration_time = time.time()
                    waiting_time_until_crash_assumed += 60
                    for corpus_name in corpus_names:
                        if corpus_response_times[corpus_name] == 0:
                            name = corpus_name[:-11]
                            print "Client handling corpus ", corpus_name, " crashed", it
                            lda_object = VariationalLDA(self.corpus_dict[name], K=self.number_of_topics, eta=0.1, alpha=1,
                                                        word_index=self.word_index, normalise=1000)
                            self.shared_job_q.put(["crashed_" + corpus_name, lda_object, it, new_beta])
                    time.sleep(10)
                pass

        return beta_sum


    def normalise_beta(self, beta_sum):
        row_sums = beta_sum.sum(axis=1)
        new_beta = beta_sum / row_sums[:, np.newaxis]
        return new_beta


    def send_new_betas_to_workers(self, new_beta, it):
        count = 0
        for corpus_name in self.corpus_dict:
            new_beta_object = ["beta", new_beta, it]
            self.send_object_to_workers(new_beta_object)
            count += 1
            print "Sent", corpus_name, str(count) + "/" + str(len(self.corpus_dict)), \
                self.shared_job_q.qsize(), self.shared_job_q.full()


    def signal_workers_to_finish(self):
        for _ in self.corpus_dict:
            self.send_object_to_workers(["Finished"])


    def collect_output_files_from_workers(self, corpus_names):
        processor_names_received = []
        files = {}
        while set(corpus_names) != set(processor_names_received):
            result_not_gotten = True
            while result_not_gotten:
                try:
                    result = self.shared_result_q.get()
                    result_not_gotten = False
                except:
                    result_not_gotten = True
            name = result[0]
            if name in processor_names_received:
                self.shared_job_q.put(["Finished"])
                print name
            else:
                print name
                processor_names_received.append(name)
                # print processor_names_received
                files[name] = result[1]
        return files


    def create_output_files(self, files):
        cwd = os.getcwd()
        print "current directory " + cwd
        directory = cwd + "/results/"
        if not os.path.exists(directory):
            os.makedirs(directory)
        corpus_dict = {"individual_lda": files.keys()}
        pickle.dump(corpus_dict, open(directory + "corpus_dict.p", "wb"))
        for file_name in files:
            pickle.dump(files[file_name], open(directory + file_name + ".dict", "wb"))


    def runserver(self):

        start_time = time.time()
        corpus_names = self.send_names_to_workers()
        print corpus_names

        waiting_time_until_crash_assumed = 360
        new_beta = np.zeros((self.number_of_topics, len(self.vocabulary)))
        it = 0
        convergence_number = 0.01
        beta_diff = self.number_of_topics
        while beta_diff > convergence_number:
            iteration_start = time.time()
            print "Starting iteration",it
            beta_sum = np.zeros((self.number_of_topics, len(self.vocabulary)))
            beta_sum = self.collect_betas_from_workers(corpus_names,
                      beta_sum, new_beta, it, waiting_time_until_crash_assumed)
            old_beta = new_beta
            new_beta = self.normalise_beta(beta_sum)
            beta_diff = np.sum(abs(np.subtract(new_beta, old_beta)))

            print "sending new_betas to workers"
            if beta_diff > convergence_number:
                self.send_new_betas_to_workers(new_beta, it)

            print "iteration: ", it, "beta difference: ", \
                beta_diff, "seconds taken:", time.time()- iteration_start
            it += 1


        print "Finished converging Beta " + str(beta_diff)
        self.signal_workers_to_finish()
        files = self.collect_output_files_from_workers(corpus_names)
        print "Got back all files " + str(len(files))
        self.create_output_files(files)

        end_time = time.time()
        print end_time - start_time, " seconds"
        time.sleep(5)

        self.manager.shutdown()


if __name__ == '__main__':
    port = 8001
    directory_paths = ["C:\LDA\LDA"]
    if len(sys.argv) > 1:
        directory_paths = sys.argv[1:]
    master = Master(port, directory_paths)
    master.runserver()
