import numpy


def generate_word_topic_distribution():
    beta = []
    topic_sum = []
    for topic in range(k):
        beta.append([])
        sum = 0
        for word in range(vocab_length):
            if (vocab_length / k * topic <= word and word < vocab_length / k * (topic + 1)):
                randint = numpy.random.random()
                beta[topic].append(randint)
                sum += randint
            else:
                beta[topic].append(0)
        topic_sum.append(sum)
    for topic in range(k):
        for word in range(vocab_length):
            beta[topic][word] = beta[topic][word] / topic_sum[topic]
    return beta;


# Generate Document
k = 5  # topics
vocabulary = ["president", "vote", "corruption", "government",
              "books", "exam", "lecture", "student",
              "bishop", "knight", "king", "pawn",
              "actor", "movie", "TV", "cinema",
              "goal", "football", "goalkeeper", "penalty"]
vocab_length = len(vocabulary)
beta = generate_word_topic_distribution()
alpha = 0.1  # concentration of topics per document
N = 100  # documents
words_per_document = 50
document_list = []
for j in range(N):
    document_words = []
    theta = numpy.random.dirichlet([alpha, alpha, alpha, alpha, alpha])

    for word in range(words_per_document):
        topic_sample = numpy.random.choice(theta)

        for i in range(k):
            if theta[i] == topic_sample:
                value = numpy.random.choice(beta[i])
                document_words.append(vocabulary[beta[i].index(value)])

    document_list.append(document_words)

for document in document_list:
    print document
