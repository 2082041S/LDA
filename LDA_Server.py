from functools import partial
from Queue import Queue as _Queue
from multiprocessing.managers import SyncManager
import time
import cPickle as pickle
import os
import numpy as np
import LDA_Config
from lda import VariationalLDA
import pdb

# Setting up the distributed system is inspired from:
# http://eli.thegreenplace.net/2012/01/24/distributed-computing-in-python-with-multiprocessing

# This class implements a pickable queue. It is needed for running the
# system shares queue.Queue objects which cannot be pickled by Windows
# between master and workers using a server proxy.
# This solution is taken from:
# http://stackoverflow.com/questions/25631266/cant-pickle-class-main-jobqueuemanager
class Queue(_Queue):
    """ A picklable queue. """

    def __getstate__(self):
        # Only pickle the state we care about
        return self.maxsize, self.queue, self.unfinished_tasks,

    def __setstate__(self, state):
        # Re-initialize the object, then overwrite the default state with
        # our pickled state.
        Queue.__init__(self)
        self.maxsize = state[0]
        self.queue = state[1]
        self.unfinished_tasks = state[2]


# Gets a queue object.
# It is a top level function in order to make Queue objects pickable
def get_q(q):
    return q


# SyncManager is subclass of BaseManager which can be used
# for the synchronization of processes. JobQueueManager will
# register a shared_job_q for the master to send work for the
# clients and a shared_results_q for the workers to send back
# results to the master.
# This class is at top level so that Queue can be pickled.
class JobQueueManager(SyncManager):
    pass


# Creates a Manager Object that controls a server process which holds a
# job queue for and allows other processes to manipulate them using proxies.
# The implementation is based on the example given within the multiprocessing
# documentation under "16.6.2.7.2. Using a remote manager"
def make_server_manager(host, port, authkey):
    job_q = Queue()
    result_q = Queue()
    server_iteration_q = Queue()
    client_crash_q = Queue()
    # get_job and get_result_q return synchronized proxies for the actual Queue
    # objects. Partial is used instead of lambda because lambda is not pickable
    JobQueueManager.register('get_job_q', callable=partial(get_q, job_q))
    JobQueueManager.register('get_result_q', callable=partial(get_q, result_q))
    JobQueueManager.register('get_server_iteration_q', callable=partial(get_q, server_iteration_q))
    JobQueueManager.register('get_client_crash_q', callable=partial(get_q, client_crash_q))
    # The manager listens at the given port and requires an
    # authentication password from the client
    manager = JobQueueManager(address=(host, port), authkey=authkey)
    manager.start()
    print'Server hostname: %s' % host
    print'Server started at port %s' % port
    return manager


# Loops over the files from a given list of  directories and creates
# a dictionary of corpora from the experiment files (end in ".dict")
def load_corpus_dict(directory_paths):
    corpus_dict = {}
    for directory in directory_paths:
        file_list = os.listdir(directory)
        for file_name in file_list:
            if file_name.endswith(".dict"):
                corpus = pickle.load(open(directory + "/" + file_name, "rb"))
                # remove ".dict"
                corpus_dict[file_name[:-5]] = corpus
    if corpus_dict == {}:
        raise ValueError("There are no corpora files within", directory_paths)
    return corpus_dict


def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False


# checks if the word is of correct format
def word_has_right_format(word):
    is_right_format = True
    if "_" not in word:
        is_right_format = False
    else:
        split_list = word.split("_")
        if len(split_list) != 2:
            is_right_format = False
        else:
            first_part = split_list[0]
            second_part = split_list[1]
            if first_part != "loss" and first_part != "fragment":
                is_right_format = False
            elif not isfloat(second_part):
                is_right_format = False
    return is_right_format


# checks if the document name is of correct format
def document_has_right_format(document):
    is_right_format = True
    if "_" not in document:
        is_right_format = False
    else:
        split_list = document.split("_")
        if len(split_list) != 2:
            is_right_format = False
        else:
            first_part = split_list[0]
            second_part = split_list[1]
            if not isfloat(first_part):
                is_right_format = False
            elif not isfloat(second_part):
                is_right_format = False
    return is_right_format


# Normalises the given beta matrix line by line
def normalise_beta(beta_sum):
    row_sums = beta_sum.sum(axis=1)
    new_beta = beta_sum / row_sums[:, np.newaxis]
    return new_beta


# Pickles the final result of each corpora i.e the lda_dictionary
# within an output file under the corpora name. It also stores the
# names of all corpora files created inside "corpus_dict.p"
def create_output_files(files):
    cwd = os.getcwd()
    print "current directory " + cwd
    directory = cwd + "/results/"
    if not os.path.exists(directory):
        os.makedirs(directory)
    corpus_dict = {"individual_lda": files.keys()}
    pickle.dump(corpus_dict, open(directory + "corpus_dict.p", "wb"))
    for file_name in files:
        pickle.dump(files[file_name], open(directory + file_name + ".dict", "wb"))


# Fault Tolerance function that checks if any worker client has crashed
# Master Server assumes a worker has crashed if crash_assumed_timer seconds
# have passed since iteration start. If master received a response back from
# any worker then it checks if crash_assumed_timer seconds have passed since then
# returns true if crash assumed
def check_if_worker_crashed(begin_iteration_time, first_result_time, crash_assumed_timer):
    if first_result_time == 0:
        iteration_timer = time.time() - begin_iteration_time
        worker_crashed_assumed = iteration_timer > crash_assumed_timer
    else:
        time_since_first_result = time.time() - first_result_time
        worker_crashed_assumed = time_since_first_result > crash_assumed_timer
    return worker_crashed_assumed


class Master:
    def __init__(self):
        self.config_data = LDA_Config.get_server_data()
        self.manager = make_server_manager(self.config_data.host,
                                           self.config_data.port,
                                           self.config_data.authkey)
        self.shared_job_q = self.manager.get_job_q()
        self.shared_result_q = self.manager.get_result_q()
        self.server_iteration_q = self.manager.get_server_iteration_q()
        self.client_crash_q = self.manager.get_client_crash_q()
        self.corpus_dict = load_corpus_dict(self.config_data.directory_paths)
        print "Loaded input files"
        self.vocabulary = self.construct_vocabulary()
        self.test_input_data()
        print "Constructed vocabulary of size", len(self.vocabulary)

        # words that appear less than this value are removed
        document_appearance_min_threshold = self.config_data.document_appearance_min_threshold
        # vocabulary of size greater than this risk breaking the system
        max_vocabulary_length = self.config_data.max_vocabulary_length
        self.reduce_vocabulary_if_needed(document_appearance_min_threshold, max_vocabulary_length)

        self.word_index = self.calculate_word_index()
        self.K = self.config_data.K

    # creates vocabulary from corpora
    def construct_vocabulary(self):
        vocabulary_dict = {}
        for corpus_name in self.corpus_dict:
            corpus = self.corpus_dict[corpus_name]
            for document in corpus:
                for word in corpus[document]:
                    vocabulary_dict[word] = True

        return vocabulary_dict.keys()

    def test_vocabulary_word_format(self):
        success = True
        error_message = ""
        for word in self.vocabulary:
            is_right_format = word_has_right_format(word)
            if not is_right_format:
                error_message = "Vocabulary word does not have right format: " + word
                success = False
                break
        return success, error_message

    def test_vocabulary_for_duplicity(self):
        return len(self.vocabulary) == len(set(self.vocabulary))

    def test_corpus(self):
        vocab = {}
        success = True
        error_message = ""
        for corpus_name in self.corpus_dict:
            words_added = 0
            corpus = self.corpus_dict[corpus_name]
            for document_name in corpus:
                document = corpus[document_name]
                is_right_format = document_has_right_format(document_name)
                if not is_right_format:
                    success = False
                    error_message = "Document name within corpus " + corpus_name + \
                                    " does not have right format: " + document_name
                    break
                for word in document:
                    if word not in vocab:
                        vocab[word] = True
                        words_added += 1
                    is_right_format = word_has_right_format(word)
                    if not is_right_format:
                        success = False
                        error_message = "Word within corpus " + corpus_name + \
                                        " within document " + document_name + \
                                        " does not have right format: " + word
                        break
                    intensity = document[word]
                    if not isfloat(intensity):
                        success = False
                        error_message = "Word " + word + " within corpus " + \
                                        corpus_name + " within document " + document_name + \
                                        " does not have right intensity: " + str(intensity)

        return success, error_message

    def test_input_data(self):
        vocabulary_format_success, error_message = self.test_vocabulary_word_format()
        if not vocabulary_format_success:
            raise ValueError(error_message)

        duplicity_test_success = self.test_vocabulary_for_duplicity()
        if not duplicity_test_success:
            raise ValueError("Vocabulary contains duplicate elements")

        corpus_format_success, error_message = self.test_corpus()
        if not corpus_format_success:
            raise ValueError(error_message)

    # if there are too many words in the vocabulary then
    # the system must remove words that appear infrequently in documents
    def reduce_vocabulary_if_needed(self, document_appearance_min_threshold, max_vocabulary_length):
        init_length = len(self.vocabulary)
        number_of_words = 0
        number_of_reduced_words = 0

        if len(self.vocabulary) > max_vocabulary_length:
            # create a smaller new vocabulary
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

            # remove words from documents that do not appear in new vocabulary
            for corpus_name in self.corpus_dict:
                corpus = self.corpus_dict[corpus_name]
                for document in corpus:
                    new_doc = {}
                    for word in corpus[document]:
                        if word in new_vocabulary_dict:
                            new_doc[word] = corpus[document][word]

                    number_of_words += len(corpus[document])
                    number_of_reduced_words += len(new_doc)
                    # print len(corpus[document]), len(new_doc)
                    corpus[document] = new_doc


            print "Reduced total number of words from", number_of_words, "to", number_of_reduced_words
            print number_of_words/number_of_reduced_words, "times less words"
            self.vocabulary = new_vocabulary_dict.keys()
            print "Reduced vocabulary size from ", init_length, " to ", len(self.vocabulary)

    # Creates a global word index from the given vocabulary
    # so that the master knows where each of those words is in
    # the beta matrix which will be returned by each worker.
    # fragment_123.456 has to be in the same position in beta
    # in all individual LDAs from the workers
    def calculate_word_index(self):
        word_index = {}
        for pos in range(len(self.vocabulary)):
            word_index[self.vocabulary[pos]] = pos
        return word_index

    # Sends an object to the workers.
    # Object can be:
    # - corpus object so that the worker starts running LDA on it
    # - beta object for the worker to update its beta
    # - crashed corpus object so that workers can work on it
    # - ["Finished"] signalling the worker to finish
    def send_object_to_workers(self, object):
        object_not_sent = True
        while object_not_sent:
            if not self.shared_job_q.full():
                self.shared_job_q.put(object)
                object_not_sent = False
            else:
                object_not_sent = True

    # Gets object from shared_job_q
    # Object is either beta_matrix or lda_dict object
    def get_object_from_workers(self):
        object_not_received = True
        result = None
        while object_not_received:
            if not self.shared_result_q.empty():
                result = self.shared_result_q.get()
                object_not_received = False
            else:
                object_not_received = True
        return result

    # puts all corpus objects into the shared_job_q
    # it also creates a list with all corpus names
    def send_corpus_objects_to_workers(self):
        corpus_names = []
        for name in self.corpus_dict:
            corpus_name = name + "_LDA_result"
            corpus_names.append(corpus_name)
            lda_object = VariationalLDA(self.corpus_dict[name], K=self.K, eta=self.config_data.eta,
                                        alpha=self.config_data.alpha, word_index=self.word_index,
                                        normalise=self.config_data.normalise)
            corpus_object = ["corpus", corpus_name, lda_object]
            self.send_object_to_workers(corpus_object)
        return corpus_names

    # while there are corpus left to collect, the function keeps on trying to get
    # them from shared_job_q. If crash_assumed_timer econds have passed since last
    # result has been gotten then assume that crash happened and send back missing
    # corpuses
    def collect_betas_from_workers(self, corpus_names, new_beta, it, crash_assumed_timer, single_threaded_LDA_timer):
        beta_sum = np.zeros((self.K, len(self.vocabulary)))
        count = 1
        begin_iteration_time = time.time()
        first_result_time = 0
        corpus_response_times = dict.fromkeys(corpus_names, 0)
        processor_names_received = []
        iteration_execution_time = 0                
        while set(corpus_names) != set(processor_names_received):
            try:
                # result = [corpus_name, beta, LDA_execution_time]
                result = self.shared_result_q.get_nowait()

                name = result[0]
                
                if name in processor_names_received:  
                    print "Already received", name 
                else:
                    print "Received " + name + " " + str(count) + "/" + str(len(corpus_names))
                    count += 1
                    if count == 1:
                        first_result_time = time.time()
                    processor_names_received.append(name)
                    corpus_response_times[name] = time.time()

                    beta = result[1]
                    LDA_execution_time = result[2]
                    pre_beta_sum = time.time()
                    beta_sum += beta
                    LDA_execution_time += (time.time() - pre_beta_sum)
                    iteration_execution_time += LDA_execution_time
                    # print processor_names_received
            except:
                client_crashed_assumed = check_if_worker_crashed(begin_iteration_time, first_result_time,
                                                                 crash_assumed_timer)
                
                if client_crashed_assumed:
                    
                    begin_iteration_time = time.time()
                    first_result_time = 0
                    # if crash occured it will take system longer to run as
                    # there is one less worker. Increase crash_assumed_timer
                    crash_assumed_timer += 60
                    for corpus_name in corpus_names:
                        if corpus_response_times[corpus_name] == 0:
                            # remove "_LDA_result" from corpus_name
                            name = corpus_name[:-11]
                            print "Client handling corpus ", corpus_name, " crashed", it
                            lda_object = VariationalLDA(self.corpus_dict[name], K=self.K, eta=self.config_data.eta,
                                                        alpha=self.config_data.alpha, word_index=self.word_index,
                                                        normalise=self.config_data.normalise)
                            # signal client that corpus sent crashed
                            crashed_corpus_object = ["corpus", "crashed_" + corpus_name, lda_object, new_beta]
                            self.send_object_to_workers(crashed_corpus_object)       
                    self.client_crash_q.put("crash")
                    print "CRASH"
                    time.sleep(3)
                    # reset crash if previous iteration crashed
                    if not self.client_crash_q.empty():
                        self.client_crash_q.get()
        
        return beta_sum, iteration_execution_time

    # Puts a beta_object for each corpus into the shared_job_q
    def send_new_betas_to_workers(self, new_beta, it, average_LDA_execution_time):
        count = 0
        for corpus_name in self.corpus_dict:
            new_beta_object = ["beta", new_beta, it, average_LDA_execution_time]
            self.send_object_to_workers(new_beta_object)
            count += 1
            print "Sent", corpus_name, str(count) + "/" + str(len(self.corpus_dict))

    # workers will stop working and send back their results
    # once they received the "Finished" signal
    def signal_workers_to_finish(self):
        for _ in self.corpus_dict:
            self.send_object_to_workers(["Finished"])

    # gets all result objects ([result_name, result]) from workers
    # it returns dictionary with result_name as key and result as value
    def collect_output_files_from_workers(self, corpus_names):
        processor_names_received = []
        files = {}
        while set(corpus_names) != set(processor_names_received):
            result = self.get_object_from_workers()
            name = result[0]

            # If same worker sends the result twice it means it has taken
            # the signal from another worker. Disregard it  and send back
            # signal to workers to finish
            if name in processor_names_received:
                self.send_object_to_workers(["Finished"])
            else:
                processor_names_received.append(name)
                files[name] = result[1]
        return files

    # runs multifile LDA algorithm and outputs the results
    def runserver(self):
        single_threaded_LDA_timer = 0
        start_execution_time = time.time()
        corpus_names = self.send_corpus_objects_to_workers()
        print corpus_names
        crash_assumed_timer = 300
        new_beta = np.zeros((self.K, len(self.vocabulary)))
        it = 0
        convergence_number = 0.01
        beta_diff = self.K
        while beta_diff > convergence_number:

            iteration_start = time.time()
            print "Starting iteration", it

            beta_sum, iteration_execution_time = self.collect_betas_from_workers(corpus_names, new_beta, it, crash_assumed_timer, single_threaded_LDA_timer)
            single_threaded_LDA_timer += iteration_execution_time
            average_LDA_execution_time = iteration_execution_time / len(corpus_names)
            
            old_beta = new_beta
            new_beta = normalise_beta(beta_sum)
            beta_diff = np.sum(abs(np.subtract(new_beta, old_beta)))

            print "sending new_betas to workers"
            if beta_diff > convergence_number:
                self.send_new_betas_to_workers(new_beta, it, average_LDA_execution_time)

            print "single threaded LDA timer:",int(iteration_execution_time),"seconds"
            print "iteration: ", it, "beta difference: ", \
                beta_diff, "seconds taken:", int(time.time() - iteration_start)

            if it >0:    
                self.server_iteration_q.put(it)        
            it += 1

        print "Finished converging Beta " + str(beta_diff)
        self.signal_workers_to_finish()
        files = self.collect_output_files_from_workers(corpus_names)
        print "Got back all files " + str(len(files))

        create_output_files_timer = time.time()
        create_output_files(files)
        create_output_files_execution_time = time.time() - create_output_files_timer
        single_threaded_LDA_timer += create_output_files_execution_time

        end_execution_time = time.time()
        print "Parallel LDA ran in", int(end_execution_time - start_execution_time), "seconds "
        print "Single Threaded LDA would have run in", int(single_threaded_LDA_timer), "seconds"
        time.sleep(5)

        self.manager.shutdown()

if __name__ == '__main__':
    Master().runserver()
