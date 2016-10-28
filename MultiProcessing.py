import multiprocessing
import time
import numpy as np
from sklearn.preprocessing import normalize
from LDA_Object import get_number_of_topics
from LDA_Object import get_vocabulary
from LDA_Object import get_initial_beta
from LDA_Object import ldaObject
from LDA_Object import get_word_in_vocab_dict


class my_processor (multiprocessing.Process):
    def __init__(self, lda, iter, processes, finished):
        multiprocessing.Process.__init__(self, target=self.f)
        self.lda = lda
        self.iter = iter
        self.processes = processes
        self.finished = finished
        self.beta = np.zeros((number_of_topics, len(vocabulary)))

    def f(self):
        for i in range(self.iter):
            count = 0
            self.lda.run_LDA(1, 5)
            self.finished[i] = True
            wait = True
            while wait:
                count += 1
                wait = False
                for p in self.processes:
                    if p.finished[i] is False:
                      wait = True
            for p in processes:
                self.beta += p.get_beta()
            self.beta = normalize(self.beta, axis=1, norm="l1")

            for p in processes:
                p.update_beta(self.beta)

    def run_LDA_once(self):
        self.lda.run_LDA(1,5)
        #print self.name, self.threadID, "finished"

    def  update_processes(self,processes):
        self.processes = list(processes)

    def update_beta(self, beta):
        self.lda.update_beta(beta)

    def get_beta(self):
        return self.lda.beta


if __name__ == "__main__":
    print multiprocessing.cpu_count()
    start_time = time.time()
    processes = []
    corpus_size = 4
    iterations = 20
    initial_beta = get_initial_beta()
    number_of_topics = get_number_of_topics()
    vocabulary = get_vocabulary()
    word_in_vocab_dict = get_word_in_vocab_dict()
    finished = ["False"]*iterations
    for c in range(corpus_size):
        p = my_processor(ldaObject([[]], {}, True), iterations, processes, finished)
        processes.append(p)

    for p in processes:
        p.update_processes(processes)

    for p in processes:
        p.start()
    # for it in range(iterations):
    #     beta = np.zeros((number_of_topics, len(vocabulary)))
    #     for p in processes:
    #         p.run_LDA_once()
    #     for p in processes:
    #         p.join()
    #     for p in processes:
    #         beta += p.get_beta()
    #     beta = normalize(beta, axis=1, norm="l1")
    #
    #     for p in processes:
    #         p.update_beta(beta)
    # end_time = time.time()
    #
    # beta_difference = np.sum(abs(np.subtract(beta, initial_beta)))
    # print beta_difference
    end_time = time.time()

    print "Finished in ", end_time - start_time, "seconds"