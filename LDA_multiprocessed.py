import numpy as np
import multiprocessing
from multiprocessing.managers import BaseManager
import time

from LDA_Object import get_number_of_topics
from LDA_Object import get_vocabulary
from LDA_Object import get_initial_beta
from LDA_Object import ldaObject
from LDA_Object import get_word_in_vocab_dict
from sklearn.preprocessing import normalize


class MyManager(BaseManager): pass


def Manager():
    m = MyManager()
    m.start()
    return m 


class LDA(object):

    def __init__(self, number_of_topics, vocabulary_length):
        self.new_beta = np.zeros((number_of_topics, vocabulary_length))
        self.current_beta = np.zeros((number_of_topics, vocabulary_length)) # zeros to test against it
        self.counter = 0

    def add_to_new_beta(self, beta):
        self.new_beta += beta

    def get_new_beta(self):
        return self.new_beta

    def get_current_beta(self):
        return self.current_beta

    def set_current_beta(self,beta):
        self.current_beta = beta

    def increment_counter(self):
        self.counter += 1

    def update_counter(self, c):
        self.counter = c

    def get_counter(self):
        return self.counter

MyManager.register('LDA', LDA)


def update(lda_proxy, lda_object, number_of_processes, iterations, l):

    while iterations > 0:

        current_process = multiprocessing.current_process()
        wait_counter =0
        current_beta = lda_proxy.get_current_beta()
        if abs(current_beta[0][0] -0.0) > 0.001:
            lda_object.update_beta(lda_proxy.get_current_beta())
        lda_object.run_LDA()
        l.acquire()
        lda_proxy.increment_counter()
        lda_proxy.add_to_new_beta(lda_object.beta)  # computation
        l.release()

        while lda_proxy.get_counter() < number_of_processes:
            pass
            #print multiprocessing.current_process().name
            #print number_of_processes," ", multiprocessing.current_process()," ",counter_proxy.get_counter()

        # compute new beta
        computed_beta = normalize(lda_proxy.get_new_beta(), axis=1, norm="l1")
        lda_proxy.set_current_beta(computed_beta)

        l.acquire()
        lda_proxy.update_counter(0)
        iterations -= 1
        #print iterations
        l.release()

    return lda_proxy


def main():
    start_time = time.time()
    corpus_size = 4
    iterations = 100
    initial_beta = get_initial_beta()
    number_of_topics = get_number_of_topics()
    vocabulary = get_vocabulary()
    word_in_vocab_dict = get_word_in_vocab_dict()

    manager = Manager()
    lda = manager.LDA(number_of_topics, len(vocabulary))
    m = multiprocessing.Manager()
    l = m.Lock()
    computed_beta=[[]]

    pool = multiprocessing.Pool(corpus_size)
    for i in range(corpus_size):
        lda_object = ldaObject([[]], {}, True)
        pool.apply_async(func=update, args=(lda, lda_object, corpus_size, iterations, l))
    pool.close()
    pool.join()

    #computed_beta = normalize(lda.get_new_beta(), axis=1, norm="l1")
    beta_difference = np.sum(abs(np.subtract(lda.get_current_beta(), initial_beta)))
    print beta_difference
    end_time = time.time()
    print "Finished in ", end_time - start_time, "seconds"

if __name__ == '__main__':
    main()

#   for it in iterations
    #   for process in processes:
    #   update shared global variable
    #   wait for all to finish
    #   use global variable as input to processes and start again