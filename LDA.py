import numpy
import time
from scipy import special


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
    for topic_index in range(number_of_topics):
        for word_index in range(vocab_length):
            topic_word_distribution[topic_index][word_index] = topic_word_distribution[topic_index][word_index] / \
                                                               sum_of_distributions[topic_index]
    return topic_word_distribution


# Generate Document
start = time.time()
number_of_topics = 5  # topics
vocabulary = ["president", "vote", "corruption", "government",
              "books", "exam", "lecture", "student",
              "bishop", "knight", "king", "pawn",
              "actor", "movie", "TV", "cinema",
              "goal", "football", "goalkeeper", "penalty"]
vocab_length = len(vocabulary)
initial_beta = generate_topic_distribution_of_words(False)  # TODO change it to numpy array

alpha = 0.1  # concentration of topics per document get ALPHA UPDATE
number_of_documents = 100  # documents
words_per_document = 50
dictionary_of_documents = {}

# for each document
for i in range(number_of_documents):
    document_dict = {}
    # sample theta ~ Dir(alpha,alpha,alpha,alpha)
    theta = numpy.random.dirichlet([alpha, alpha, alpha, alpha, alpha])
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

end = time.time()

print "Preprocessing takes ", end - start, " seconds"
# for document in dictionary_of_documents:
# print dictionary_of_documents[document]

# Implement LDA
# Step 1  Initialise
beta = generate_topic_distribution_of_words(True)  # topic X vocab_length
word_in_vocab_dict = {}
for index in range(vocab_length):
    word_in_vocab_dict[vocabulary[index]] = index;

document_parameters = {}
# for each document create document_per_topic_distribution and word_per topic distribution
for id in range(len(dictionary_of_documents)):
    gamma = numpy.random.uniform(low=0.0, high=1.0, size=5)
    # By reversing parameters of function we get wor_per_topic distribution
    phi = generate_word_distribution_of_topics(dictionary_of_documents[id])
    document_parameters[id] = [gamma, phi]

# print document_parameters[0]

# Step 2 Run algorithm

iterations = 10
for it in range(iterations):
    for n in range(number_of_documents):
        gamma = document_parameters[n][0]  # document per topic distribution
        document = dictionary_of_documents[n]
        Ln = {}
        phi = {}

        # for each word recalculate topic probability
        for word in document:
            Ln[word] = numpy.zeros(number_of_topics)
            phi[word] = numpy.zeros(number_of_topics)

            for j in range(number_of_topics):
                Ln[word][j] = (
                numpy.log(beta[j][word_in_vocab_dict[word]]) + special.psi(gamma[j]) - special.psi(sum(gamma)))

            B = - numpy.max(Ln[word])
            exponential_Lnj_plus_B = []

            for j in range(number_of_topics):
                exponential_Lnj_plus_B.append(numpy.exp(Ln[word][j] + B))

            for j in range(number_of_topics):
                phi[word][j] = exponential_Lnj_plus_B[j] / numpy.sum(exponential_Lnj_plus_B)

        # for each document recalculate topic distribution
        for j in range(number_of_topics):
            topic_probability = 0

            for word in document:
                word_probability = phi[word][j]
                word_count = document[word]
                topic_probability += word_probability * word_count

            gamma[j] = alpha + topic_probability

        # update document parameters
        document_parameters[n][0] = gamma
        document_parameters[n][1] = phi

    # initialise new beta
    for j in range(number_of_topics):
        for i in range(vocab_length):
            beta[j][i] = 0.01  # Epsilon

    # recalculate beta
    for n in range(number_of_documents):
        phi = document_parameters[n][1]

        for word in dictionary_of_documents[n]:

            for j in range(number_of_topics):
                beta[j][word_in_vocab_dict[word]] += phi[word][j]

    for j in range(number_of_topics):
        topic_sum = sum(beta[j])
        for i in range(vocab_length):
            beta[j][i] /= topic_sum

print "Comparing initial beta with LDA computed beta"
for i in range(number_of_topics):
    print "Topic ", i, ":"
    print initial_beta[i]
    print beta[i]

end2 = time.time()
print "Running the algorithm ", iterations, " times takes ", end2 - end, " seconds"
