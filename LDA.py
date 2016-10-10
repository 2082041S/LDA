import numpy


def index_between_bounds(index, topic_index , vocab):
    greater_than_lower_bound = vocab_length * topic_index / number_of_topics <= index
    lower_than_upper_bound = index < vocab_length * (topic_index + 1) / number_of_topics
    between_bounds = greater_than_lower_bound and lower_than_upper_bound
    return between_bounds


def generate_topic_distribution_of_words(topics, vocab, random):
    topic_word_distribution = []
    sum_of_distributions = []

    for topic_index in range(topics):
        topic_word_distribution.append([])
        sum_of_word_probabilities = 0

        for word_index in range(vocab):
            randint = numpy.random.random()

            # generate random word_topic distribution
            if random:
                topic_word_distribution[topic_index].append(randint)
                sum_of_word_probabilities += randint
            # generate topics divergent: topic 1 is determined by words 0 to 3 topic topic 2 by words 4 to7
            elif index_between_bounds(word_index, topic_index,vocab):
                topic_word_distribution[topic_index].append(randint)
                sum_of_word_probabilities += randint
            else:
                topic_word_distribution[topic_index].append(0)
        sum_of_distributions.append(sum_of_word_probabilities)

    # normalise the distribution
    for topic_index in range(topics):
        for word_index in range(vocab):
            topic_word_distribution[topic_index][word_index] = topic_word_distribution[topic_index][word_index] / \
                                                               sum_of_distributions[topic_index]
    return topic_word_distribution


# Generate Document
number_of_topics = 5  # topics
vocabulary = ["president", "vote", "corruption", "government",
              "books", "exam", "lecture", "student",
              "bishop", "knight", "king", "pawn",
              "actor", "movie", "TV", "cinema",
              "goal", "football", "goalkeeper", "penalty"]
vocab_length = len(vocabulary)
beta = generate_topic_distribution_of_words(number_of_topics, vocab_length, False)
alpha = 0.1  # concentration of topics per document
number_of_documents = 100  # documents
words_per_document = 50
list_of_documents = []

# for each document
for i in range(number_of_documents):
    document_dict = {"id": i}
    # sample theta ~ Dir(alpha,alpha,alpha,alpha)
    theta = numpy.random.dirichlet([alpha, alpha, alpha, alpha, alpha])
    # error check
    if abs(sum(theta) - 1) > 0.01:
        print "theta not normalised"

    for j in range(words_per_document):
        # choose topic by sampling theta
        topic_sample_index = numpy.random.choice(range(number_of_topics), p=theta)

        # choose word by sampling from chosen topic
        word_sample_index = numpy.random.choice(range(vocab_length), p=beta[topic_sample_index])
        sampled_word = vocabulary[word_sample_index]
        if sampled_word not in document_dict:
            document_dict[vocabulary[word_sample_index]] = 1
        else:
            document_dict[sampled_word] += 1

    list_of_documents.append(document_dict)

# for document in list_of_documents:
#   print document

# Implement LDA
# Step 1  Initialise
beta = generate_topic_distribution_of_words(number_of_topics, vocab_length, True)
document_parameters = {}
# for each document create document_per_topic_distribution and word_per topic distribution
for document in list_of_documents:
    random_list = numpy.random.uniform(low=0.0, high=1.0, size=5)
    topic_distribution_of_document = [i / sum(random_list) for i in random_list]  # Gamma
    # By reversing parameters of function we get wor_per_topic distribution
    word_per_topic_distribution = generate_topic_distribution_of_words(vocab_length, number_of_topics, True)  # O|
    document_parameters[document["id"]] = [topic_distribution_of_document, word_per_topic_distribution]

# print document_parameters

# Step 2 Run algorithm
# loop over each document
for n in range(number_of_documents):
    document_id = list_of_documents[n]["id"]
    topic_distribution_of_document = document_parameters[document_id][0]
    word_per_topic_distribution = document_parameters[document_id][1]
    # for each word in document
    i = 0
    for i in range(len(list_of_documents[n])):
        i += 1
        # TODO calculate exp
        exp = 1
        for j in range(number_of_topics):
            # for each word recalculate probability.
            # Eq (6):
            word_per_topic_distribution[i][j] = beta[j][i] * exp

    # for each document recalculate topic distribution
    for j in range(number_of_topics):
        sum = 0

        for v in range(len(word_per_topic_distribution)):
            word = vocabulary[v]
            if word in list_of_documents[n]:
                word_probability = word_per_topic_distribution[v][j]
                word_count = list_of_documents[n][word]
                sum += word_probability * word_count

        # Eq (7):
        topic_distribution_of_document[j] = alpha + sum

    # update document parameters
    document_parameters[document_id][0] = topic_distribution_of_document
    document_parameters[document_id][1] = word_per_topic_distribution
