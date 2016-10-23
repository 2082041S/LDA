import numpy as np
import time
from scipy import special
from sklearn.preprocessing import normalize
from scipy.linalg import norm

number_of_topics = 5
vocabulary = ["president", "vote", "corruption", "government",
              "books", "exam", "lecture", "student",
              "bishop", "knight", "king", "pawn",
              "actor", "movie", "TV", "cinema",
              "goal", "football", "goalkeeper", "penalty"]

word_in_vocab_dict = {}
for index in range(len(vocabulary)):
    word_in_vocab_dict[vocabulary[index]] = index;

initial_beta = np.random.rand(number_of_topics,len(vocabulary))
for j in range(len(initial_beta)):
    topic = initial_beta[j]
    initial_beta[j] = map(lambda x: x/sum(topic), topic)


def get_word_in_vocab_dict():
    return word_in_vocab_dict


def get_initial_beta():
    return initial_beta


def get_number_of_topics():
    return number_of_topics


def get_vocabulary():
    return vocabulary


def generate_corpus():
    alpha = np.random.uniform(low=0.0, high=1.0, size=5)
    number_of_documents = 50
    words_per_document = 50
    # print "corpus has ", number_of_documents, " documents each containing ", \
    #    words_per_document, " words with alpha equal to: ", alpha
    corpus = []

    for i in range(number_of_documents):
        document_dict = {}
        theta = np.random.dirichlet(alpha)
        for j in range(words_per_document):
            topic_sample_index = np.random.choice(range(number_of_topics), p=theta)
            word_sample_index = np.random.choice(range(len(vocabulary)), p=initial_beta[topic_sample_index])
            sampled_word = vocabulary[word_sample_index]
            if sampled_word not in document_dict:
                document_dict[vocabulary[word_sample_index]] = 1
            else:
                document_dict[sampled_word] += 1

        corpus.append(document_dict)
    return corpus, alpha


def generate_word_distribution_of_topics(words_in_document):
    word_topic_dict = {}
    for word in words_in_document:
        word_topic_dict[word] = np.random.uniform(low=0.0, high=1.0, size=5)
    return word_topic_dict


def initialize_phi_and_gamma_for_corpus(corpus):
    document_parameters = []
    for document in corpus:
        gamma = np.random.uniform(low=0.0, high=1.0, size=5)
        # By reversing parameters of function we get wor_per_topic distribution
        phi = generate_word_distribution_of_topics(document)
        document_parameters.append([gamma, phi])
    return document_parameters


def calculate_phi_and_new_beta(beta, gamma, document, new_beta):
    Ln = {}
    phi = {}
    for word in document:
        Ln[word] = np.zeros(number_of_topics)
        phi[word] = np.zeros(number_of_topics)
        for j in range(number_of_topics):
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
    gamma = {}
    for j in range(number_of_topics):
        topic_probability = 0

        for word in document:
            word_probability = phi[word][j]
            word_count = document[word]
            topic_probability += word_probability * word_count

        gamma[j] = alpha[j] + topic_probability

    return gamma


class ldaObject:

    def __init__(self, beta, corpus, random):
        if beta == [[]]:
            random_matrix = np.random.rand(number_of_topics,len(vocabulary))
            for j in range(len(random_matrix)):
                topic = random_matrix[j]
                random_matrix[j] = map(lambda x: x / sum(topic), topic)
            self.beta = random_matrix
        else:
            self.beta = beta
        if random:
            self.corpus, self.alpha = generate_corpus()
        else:
            self.corpus = corpus
        self.document_parameters = initialize_phi_and_gamma_for_corpus(self.corpus)

    def update_beta(self, beta):
        self.beta = beta

    def run_LDA(self, iterations=1, phi_and_gamma_iterations=1):
        epsilon = 0.01
        for it in range(iterations):
            for i in range(phi_and_gamma_iterations):
                new_beta = np.full((number_of_topics, len(vocabulary)), epsilon)
                for n in range(len(self.corpus)):
                    gamma = self.document_parameters[n][0]  # document per topic distribution
                    document = self.corpus[n]

                    # for each document recalculate topic probability Eq (6)
                    phi, new_beta = calculate_phi_and_new_beta(self.beta, gamma, document, new_beta)

                    # for each document recalculate topic distribution Eq (7)
                    gamma = calculate_gamma(self.alpha, document,phi)

                    # update document parameters
                    self.document_parameters[n][0] = gamma
                    self.document_parameters[n][1] = phi

            # Normalise new computed beta and assign it to beta
            new_beta = normalize(new_beta, axis=1, norm="l1")
            #beta_difference = np.sum(abs(np.subtract(new_beta, self.beta)))
            self.beta = new_beta.copy()
            #print np.abs(beta_difference)


#lda = ldaObject([[]], {}, True)
#lda.run_LDA(100,1)