import numpy as np
import time
from scipy import special
import pickle

initial_beta = pickle.load(open("initial_beta.p","rb"))
number_of_topics = len(initial_beta)
vocabulary = pickle.load(open("vocabulary.p","rb"))
corpus_list = pickle.load(open("corpus.p","rb"))
alpha_list = pickle.load(open("alpha.p","rb"))
word_in_vocab_dict = {}
for index in range(len(vocabulary)):
    word_in_vocab_dict[vocabulary[index]] = index;


# normalise it
for j in range(len(initial_beta)):
    topic = initial_beta[j]
    initial_beta[j] = map(lambda x: x/sum(topic), topic)




def generate_word_distribution_of_topics(words_in_document):
    word_topic_dict = {}
    for word in words_in_document:
        word_topic_dict[word] = np.random.uniform(low=0.0, high=1.0, size=number_of_topics)
    return word_topic_dict


def initialize_phi_and_gamma_for_corpus(corpus):
    document_parameters = []
    for document in corpus:
        gamma = np.random.uniform(low=0.0, high=1.0, size=number_of_topics)
        phi = generate_word_distribution_of_topics(corpus[document])
        document_parameters.append([gamma, phi])
    return document_parameters


def calculate_phi_and_new_beta(beta, gamma, document, new_beta):
    Ln = {}
    phi = {}
    for word in document:
        Ln[word] = np.zeros(number_of_topics)
        phi[word] = np.zeros(number_of_topics)
        for j in range(number_of_topics):
            #print beta[j]
            Ln[word][j] = np.log(beta[j][word_in_vocab_dict[word]]) + \
                          special.psi(gamma[j]) - special.psi(sum(gamma))
        B = - np.max(Ln[word])
        exponential_Lnj_plus_B = []
        for j in range(number_of_topics):
            exponential_Lnj_plus_B.append(np.exp(Ln[word][j] + B))
        exponential_sum = np.sum(exponential_Lnj_plus_B)
        for j in range(number_of_topics):
            phi[word][j] = exponential_Lnj_plus_B[j] / exponential_sum
            new_beta[j][word_in_vocab_dict[word]] += phi[word][j] * document[word]

    return phi,new_beta


def calculate_gamma(alpha, document, phi):
    gamma = []
    for j in range(number_of_topics):
        topic_probability = 0

        for word in document:
            word_probability = phi[word][j]
            word_count = document[word]
            topic_probability += word_probability * word_count

        gamma.append(alpha[j] + topic_probability)

    return gamma


def calculate_new_alpha(init_alpha, gamma_list, max_iter = 1):
    alpha = np.copy(init_alpha)
    M = len(gamma_list)
    K = number_of_topics
    g = np.zeros(K)
    g_sum_gamma = np.zeros(K)
    for i in range (K):
        for d in range (M):
            g_sum_gamma[i] += special.psi(gamma_list[d][i]) - special.psi(sum(gamma_list[d]))

    for it in range(max_iter):
        g = M *(special.psi(sum(alpha)) - special.psi(alpha)) + g_sum_gamma
        #H = M * (special.polygamma(1, sum(alpha)) - np.diag(special.polygamma(1, alpha)))
        z = M * special.polygamma(1, sum(alpha))
        h = -M * special.polygamma(1, alpha)
        c = sum(g/h)/ (1/z + sum(np.ones(number_of_topics)/h))

        new_alpha = alpha - ((g - np.ones(K) * c) / h)

        if (new_alpha < 0).sum() > 0:
            init_alpha = np.copy(init_alpha) / 10.0
            return calculate_new_alpha(init_alpha, gamma_list, max_iter)

        diff = np.sum(np.abs(alpha - new_alpha))
        #print diff ,it
        alpha = new_alpha
        if diff < 1e-5 and it > 1:
            return alpha

    #print alpha
    return alpha



class ldaObject:

    def __init__(self, beta_matrix, corpus, alpha):
        if beta_matrix == [[]]:
            self.beta_matrix = initial_beta
        else:
            self.beta_matrix = beta_matrix
        self.alpha = alpha
        self.corpus = corpus
        self.document_parameters = initialize_phi_and_gamma_for_corpus(self.corpus)


    def update_beta(self, beta):
        self.beta_matrix = beta

    def run_LDA(self, iterations=1, phi_and_gamma_iterations=1):
        epsilon = 0.01
        for it in range(iterations):
            for i in range(phi_and_gamma_iterations):
                new_beta = np.ones((number_of_topics, len(vocabulary))) * epsilon
                for n in range(len(self.corpus)):
                    gamma = self.document_parameters[n][0]  # document per topic distribution
                    document = self.corpus[n]
                    #print len(document), document
                    # for each document recalculate topic probability Eq (6)
                    phi, new_beta = calculate_phi_and_new_beta(self.beta_matrix, gamma, document, new_beta)

                    # for each document recalculate topic distribution Eq (7)
                    gamma = calculate_gamma(self.alpha, document,phi)

                    # update document parameters
                    self.document_parameters[n][0] = gamma
                    self.document_parameters[n][1] = phi
            # recalculate alpha
            gamma_list = map(list, zip(*self.document_parameters))[0]
            self.alpha = calculate_new_alpha(self.alpha, gamma_list,20)
            # Normalise new computed beta and assign it to beta
            row_sums = new_beta.sum(axis=1)
            new_beta = new_beta / row_sums[:, np.newaxis]

            beta_difference = np.sum(abs(np.subtract(new_beta, self.beta_matrix)))
            self.beta_matrix = new_beta.copy()
            #print np.abs(beta_difference)


def main():


    start = time.time()
    # print initial_beta
    # print corpus_list
    # print vocabulary
    # print number_of_topics

    for i in range (1):
        lda_object = ldaObject([[]], corpus_list[i], alpha_list[i])
        #lda_object = ldaObject([[]],{},[],True)
        lda_object.run_LDA(100,1)

    end = time.time()
    print end - start, "seconds"

if __name__ == '__main__':
    main()
