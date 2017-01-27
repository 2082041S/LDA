import socket
from multiprocessing.managers import SyncManager
import time
import cPickle as pickle
import os
import numpy as np
import sys
from lda import VariationalLDA
from multiprocessing import Manager
import pdb


class JobQueueManager(SyncManager):
    pass

# class Beta_Holder():
#     def __init__(self, beta=[]):
#         self.beta = beta

def make_server_manager(port, authkey):
    job_q = Manager().Queue()
    result_q = Manager().Queue()
    JobQueueManager.register('get_job_q', callable=lambda: job_q)
    JobQueueManager.register('get_result_q', callable=lambda: result_q)

    manager = JobQueueManager(address=(socket.gethostname(), port), authkey=authkey)
    manager.start()
    print'Server hostname: %s' % socket.gethostname()
    print'Server started at port %s' % port
    return manager


def load_corpus_dict(directory_paths):
    corpus_dict = {}
    for directory in directory_paths:
        file_list = os.listdir(directory)
        #vocabulary = {}
        for file_name in file_list:
            if file_name.endswith(".dict"):
                corpus = pickle.load(open(directory +"/" + file_name,"rb"))
                corpus_dict[file_name[:-5]] = corpus
                # for document in corpus:
                #     for word in corpus[document]:
                #         vocabulary[word] = True
    return corpus_dict


def construct_vocabulary(corpus_dict):
    vocabulary_dict = {}
    intensity_list = []
    for corpus_name in corpus_dict:
        max_intensity = 0
        fragment_name = ""
        corpus = corpus_dict[corpus_name]
        for document in corpus:
            for word in corpus[document]:
                vocabulary_dict[word] = True
                intensity = corpus[document][word]
                if intensity > max_intensity:
                    max_intensity = intensity
                    fragment = word
        intensity_list.append([corpus_name, max_intensity, fragment])

    intensity_list.sort()
    for el in intensity_list:
        print el
    return vocabulary_dict.keys()


def reduce_vocabulary_if_needed(corpus_dict, vocabulary):
    max_vocabulary_length = 50000
    document_appearance_min_threshold = 10
    init_length = len(vocabulary)
    # if there are too many words in the vocabulary then
    # the system must remove words that appear infrequently in documents
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
        vocabulary = new_vocabulary_dict.keys()
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
        lda_object = VariationalLDA(corpus_dict[name], K=number_of_topics, eta=0.1, alpha=1, word_index=word_index, normalise = 1000)
        shared_job_q.put([corpus_name, lda_object])
    return corpus_names


def collect_betas_from_slaves(shared_result_q, shared_job_q, corpus_dict, number_of_topics, word_index,
                              corpus_names, beta_sum, new_beta, it, waiting_time_until_crash_assumed):

    count = 1
    begin_iteration_time = time.time()
    first_result_time = 0
    corpus_response_times = dict.fromkeys(corpus_names, 0)
    processor_names_received = []
    while set(corpus_names) != set(processor_names_received):
        try:
            result = shared_result_q.get_nowait()
            name = result[0]
            print "Received " + name + " " +str(count)+ "/"+ str(len(corpus_names))
            if name in processor_names_received:
                shared_job_q.put_nowait(["beta", new_beta, it])
                # print name
            else:
                count +=1
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
                        lda_object = VariationalLDA(corpus_dict[name], K=number_of_topics, eta=0.1, alpha=1,
                                                    word_index=word_index, normalise = 1000)
                        shared_job_q.put(["crashed_" +corpus_name, lda_object, it, new_beta])
                time.sleep(10)
            pass

    return beta_sum

def normalize_beta(beta_sum):
    row_sums = beta_sum.sum(axis=1)
    new_beta = beta_sum / row_sums[:, np.newaxis]
    return new_beta


def send_new_betas_to_slaves(shared_job_q, corpus_dict, new_beta, it):
    count = 0
    for corpus_name in corpus_dict:
        result_not_put = True
        # beta_holder = Beta_Holder(new_beta)
        while result_not_put:
            try:
                shared_job_q.put(["beta", new_beta, it])
                result_not_put = False
                count += 1
                print "Sent", corpus_name, str(count)+"/"+ str(len(corpus_dict)), shared_job_q.qsize(),shared_job_q.full()
            except:
                pdb.set_trace()
                ex = sys.exc_info()[0]
                result_not_put = True


def signal_slaves_to_finish(shared_job_q, corpus_dict):
    for j in range(len(corpus_dict)):
        result_not_put = True
        while result_not_put:
            try:
                shared_job_q.put(["Finished"])
                result_not_put = False
            except:
                result_not_put = True

        


def collect_output_files_from_slaves(shared_result_q, shared_job_q, corpus_names):
    processor_names_received = []
    files = {}
    while set(corpus_names) != set(processor_names_received):
        result_not_gotten = True
        while result_not_gotten:
            try:
                result = shared_result_q.get()
                result_not_gotten = False
            except:
                result_not_gotten = True       
        name = result[0]
        if name in processor_names_received:
            shared_job_q.put(["Finished"])
            print name
        else:
            print name
            processor_names_received.append(name)
            # print processor_names_received
            files[name] = result[1]
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
    def __init__(self, port, directory_paths):
        self.corpus_dict = load_corpus_dict(directory_paths)
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

        waiting_time_until_crash_assumed = 360
        new_beta = np.zeros((self.number_of_topics, len(self.vocabulary)))
        it = 0
        convergence_number = 0.01
        beta_diff = self.number_of_topics
        while beta_diff > convergence_number:
            iteration_start = time.time()
            print "Starting iteration",it
            beta_sum = np.zeros((self.number_of_topics, len(self.vocabulary)))
            beta_sum = collect_betas_from_slaves(self.shared_result_q, self.shared_job_q, self.corpus_dict,
                                                 self.number_of_topics, self.word_index, corpus_names,
                                                 beta_sum, new_beta, it, waiting_time_until_crash_assumed)
            old_beta = new_beta
            new_beta = normalize_beta(beta_sum)
            beta_diff = np.sum(abs(np.subtract(new_beta, old_beta)))

            print "sending new_betas to workers"
            if beta_diff > convergence_number:
                send_new_betas_to_slaves(self.shared_job_q, self.corpus_dict, new_beta, it)

            print "iteration: ", it, "beta difference: ", beta_diff, "seconds taken:", time.time()- iteration_start
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
    directory_paths = ["/users/level4/2082041s/LDA/in/dicts",
                    "/users/level4/2082041s/LDA/in/dicts_1",
                    "/users/level4/2082041s/LDA/in/dicts_2",
                    "/users/level4/2082041s/LDA/in/dicts_3"]
    if len(sys.argv) > 1:
        directory_paths = sys.argv[1:]
    master = Master(port,directory_paths)
    master.runserver()
