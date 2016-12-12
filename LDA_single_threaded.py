import numpy
import time
from scipy import special
from sklearn.preprocessing import normalize


def index_between_bounds(index, topic_index):
    greater_than_lower_bound = vocab_length * topic_index / number_of_topics <= index
    lower_than_upper_bound = index < vocab_length * (topic_index + 1) / number_of_topics
    between_bounds = greater_than_lower_bound and lower_than_upper_bound
    return between_bounds


def generate_word_distribution_of_topics(words_in_document):
    word_topic_dict = {}
    for word in words_in_document:
        word_topic_dict[word] = numpy.random.uniform(low=0.0, high=1.0, size=5)
    return word_topic_dict


def generate_topic_distribution_of_words(random):
    topic_word_distribution = []
    sum_of_distributions = []

    for topic_index in range(number_of_topics):
        topic_word_distribution.append([])
        sum_of_word_probabilities = 0

        for word_index in range(vocab_length):
            randint = numpy.random.random()

            # generate random word_topic distribution
            if random:
                topic_word_distribution[topic_index].append(randint)
                sum_of_word_probabilities += randint
            # generate topics divergent: topic 1 is determined by words 0 to 3 topic topic 2 by words 4 to7
            elif index_between_bounds(word_index, topic_index):
                topic_word_distribution[topic_index].append(randint)
                sum_of_word_probabilities += randint
            else:
                topic_word_distribution[topic_index].append(0)
        sum_of_distributions.append(sum_of_word_probabilities)

    # normalise the distribution
    topic_word_distribution = normalize(topic_word_distribution, axis=1, norm="l1")
    return topic_word_distribution


def generate_corpus():
    words_generated = 0
    for c in range(corpus_size):
        alpha_collection.append(numpy.random.uniform(low=0.0, high=1.0, size=5))
        number_of_documents = 50
        words_per_document = 50
        #number_of_documents = numpy.random.randint(1, 100)  # documents
        #words_per_document = numpy.random.randint(1, 100)
        words_generated += number_of_documents * words_per_document
        print "corpus ", c , " has ", number_of_documents, " documents each containing ",\
            words_per_document, " words with alpha equal to: ",alpha_collection[c]
        dictionary_of_documents = {}
        generate_documents(alpha_collection[c], number_of_documents, words_per_document, dictionary_of_documents)
        corpus.append(dictionary_of_documents)
    return words_generated


def generate_documents(alpha, number_of_documents, words_per_document, dictionary_of_documents):
    # for each document
    for i in range(number_of_documents):
        document_dict = {}
        # sample theta ~ Dir(alpha)
        theta = numpy.random.dirichlet(alpha)
        # error check
        if abs(sum(theta) - 1) > 0.01:
            print "theta not normalised"

        for j in range(words_per_document):
            # choose topic by sampling theta
            topic_sample_index = numpy.random.choice(range(number_of_topics), p=theta)

            # choose word by sampling from chosen topic
            word_sample_index = numpy.random.choice(range(vocab_length), p=initial_beta[topic_sample_index])
            sampled_word = vocabulary[word_sample_index]
            if sampled_word not in document_dict:
                document_dict[vocabulary[word_sample_index]] = 1
            else:
                document_dict[sampled_word] += 1

        dictionary_of_documents[i] = document_dict


def generate_gamma_and_phi_for_each_document(dictionary_of_documents, document_parameters):
    for id in range(len(dictionary_of_documents)):
        gamma = numpy.random.uniform(low=0.0, high=1.0, size=5)
        # By reversing parameters of function we get wor_per_topic distribution
        phi = generate_word_distribution_of_topics(dictionary_of_documents[id])
        document_parameters[id] = [gamma, phi]


def print_evaluation_criteria():
    end_LDA = time.time()
    LDA_time = end_LDA - end_preprocess
    print "Running the algorithm ", iterations, " times takes ", LDA_time, " seconds"
    print "Words per seconds = ", (iterations * number_of_words) / LDA_time


def calculate_gamma(alpha, document):
    for j in range(number_of_topics):
        topic_probability = 0

        for word in document:
            word_probability = phi[word][j]
            word_count = document[word]
            topic_probability += word_probability * word_count

        gamma[j] = alpha[j] + topic_probability


# Preprocessing
start = time.time()
number_of_topics = 5  # topics
vocabulary = ["president", "vote", "corruption", "government",
              "books", "exam", "lecture", "student",
              "bishop", "knight", "king", "pawn",
              "actor", "movie", "TV", "cinema",
              "goal", "football", "goalkeeper", "penalty"]
vocab_length = len(vocabulary)
initial_beta = generate_topic_distribution_of_words(True)
corpus_size = 4
corpus=[]
alpha_collection=[]
number_of_words = generate_corpus()
end_preprocess = time.time()
print "Preprocessing takes ", end_preprocess - start, " seconds"

# Implement LDA
# Step 1  Initialise
random_beta = generate_topic_distribution_of_words(True)  # topic X vocab_length
word_in_vocab_dict = {}
for index in range(vocab_length):
    word_in_vocab_dict[vocabulary[index]] = index;
list_of_document_parameters = []
for c in range(corpus_size):
    document_parameters ={}
    # for each document create document_per_topic_distribution and word_per topic distribution
    generate_gamma_and_phi_for_each_document(corpus[c], document_parameters)
    list_of_document_parameters.append(document_parameters)

# Step 2 Run algorithm
iterations = 100
epsilon = 0.01

beta = random_beta
for it in range(iterations):
    # initialise new beta
    # update phi and gamma 5 times
    new_beta = numpy.full((number_of_topics, vocab_length), epsilon)
    for c in range(corpus_size):
        dictionary_of_documents = corpus[c]
        document_parameters = list_of_document_parameters[c]
        for n in dictionary_of_documents:
            gamma = document_parameters[n][0]  # document per topic distribution
            document = dictionary_of_documents[n]
            Ln = {}
            phi = {}
            # for each word recalculate topic probability Eq (6)
            for word in document:
                Ln[word] = numpy.zeros(number_of_topics)
                phi[word] = numpy.zeros(number_of_topics)
                for j in range(number_of_topics):
                    Ln[word][j] = (numpy.log(beta[j][word_in_vocab_dict[word]]) + special.psi(gamma[j]) - special.psi(sum(gamma)))
                B = - numpy.max(Ln[word])
                exponential_Lnj_plus_B = []
                for j in range(number_of_topics):
                    exponential_Lnj_plus_B.append(numpy.exp(Ln[word][j] + B))
                exponential_sum = numpy.sum(exponential_Lnj_plus_B)
                for j in range(number_of_topics):
                    phi[word][j] = exponential_Lnj_plus_B[j] / exponential_sum
                    new_beta[j][word_in_vocab_dict[word]] += phi[word][j] * document[word]
            # for each document recalculate topic distribution Eq (7)
            calculate_gamma(alpha_collection[c], document)

            # update document parameters
            document_parameters[n][0] = gamma
            document_parameters[n][1] = phi

    # Normalise new computed beta and assign it to beta
    new_beta = normalize(new_beta, axis=1, norm ="l1")
    beta_difference = numpy.sum(abs(numpy.subtract(new_beta, beta)))
    beta = new_beta.copy()
    #print beta[0][:10]
    #print numpy.abs(beta_difference)

beta_difference = numpy.sum(abs(numpy.subtract(beta, initial_beta)))
# print numpy.abs(beta_difference)
# for topic in beta:
#     for word in vocabulary:
#         if topic[word_in_vocab_dict[word]]>0.1:
#             print word, " ", topic[word_in_vocab_dict[word]]
# for topic in initial_beta:
#     for word in vocabulary:
#         if topic[word_in_vocab_dict[word]]>0.1:
#             print word, " ", topic[word_in_vocab_dict[word]]
print_evaluation_criteria()
