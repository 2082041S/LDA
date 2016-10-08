import numpy

def generate_word_topic_distribution(number_of_topics, vocab_length):
    word_topic_distribution = []
    sum_of_distributions = []

    for topic_index in range(number_of_topics):
        word_topic_distribution.append([])
        sum = 0

        for word_index in range(vocab_length):
            #make topics divergent topic 1 is determined by words 0 to 3 topic topic 2 by words 4 to7
            if (vocab_length * topic_index/ number_of_topics  <= word_index and word_index < vocab_length * (topic_index + 1)/ number_of_topics ):
                randint = numpy.random.random()
                word_topic_distribution[topic_index].append(randint)
                sum += randint
            else:
                word_topic_distribution[topic_index].append(0)
        sum_of_distributions.append(sum)

    # normalise the distribution
    for topic_index in range(number_of_topics):
        for word_index in range(vocab_length):
            word_topic_distribution[topic_index][word_index] = word_topic_distribution[topic_index][word_index] / sum_of_distributions[topic_index]
    return word_topic_distribution;


# Generate Document
number_of_topics = 5  # topics
vocabulary = ["president", "vote", "corruption", "government",
              "books", "exam", "lecture", "student",
              "bishop", "knight", "king", "pawn",
              "actor", "movie", "TV", "cinema",
              "goal", "football", "goalkeeper", "penalty"]
vocab_length = len(vocabulary)
word_topic_distribution = generate_word_topic_distribution(number_of_topics, vocab_length)
alpha = 0.1  # concentration of topics per document
number_of_documents = 100  # documents
words_per_document = 50
document_list = []

#for each document
for i in range(number_of_documents):
    document_words = []
    #sample theta ~ Dir(alpha,alpha,alpha,alpha)
    theta = numpy.random.dirichlet([alpha, alpha, alpha, alpha, alpha])
    if abs(sum(theta)- 1)>0.01:
        print "theta not normalised"

    for j in range(words_per_document):
        #choose topic by sampling theta
        topic_sample_index = numpy.random.choice(range(number_of_topics), p=theta)

        # choose word by sampling from chosen topic
        word_sample_index = numpy.random.choice(range(vocab_length), p=word_topic_distribution[topic_sample_index])
        document_words.append(vocabulary[word_sample_index])

    document_list.append(document_words)

for document in document_list:
    print document
