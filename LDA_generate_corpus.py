import numpy as np
import pickle

def generate_words(size):
    wordList =[]
    for i in range(size):
        wordList.append("word" + str(i))
    return wordList

def generate_sparse_topic_distribution_of_words():
    topic_word_distribution = []
    sum_of_distributions = []

    for topic_index in range(number_of_topics):
        topic_word_distribution.append([])

        for word_index in range(len(vocabulary)):
            randint = np.random.random()
            # generate topics divergent: topic 1 is determined by words 0 to 3 topic topic 2 by words 4 to7
            if index_between_bounds(word_index, topic_index):
                topic_word_distribution[topic_index].append(randint)
            else:
                topic_word_distribution[topic_index].append(0)

    return topic_word_distribution

def index_between_bounds(index, topic_index):
    greater_than_lower_bound = len(vocabulary) * topic_index / number_of_topics <= index
    lower_than_upper_bound = index < len(vocabulary) * (topic_index + 1) / number_of_topics
    between_bounds = greater_than_lower_bound and lower_than_upper_bound
    return between_bounds


def generate_corpus(number_of_documents, words_per_document):
    alpha = np.random.uniform(low=0.0, high=1.0, size=number_of_topics)
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


def get_initial_beta():
    return initial_beta


number_of_topics = 50
number_of_corpus = 4
vocabulary = generate_words(200)

word_in_vocab_dict = {}
for index in range(len(vocabulary)):
    word_in_vocab_dict[vocabulary[index]] = index;

#initial_beta = generate_sparse_topic_distribution_of_words()
initial_beta = np.random.rand(number_of_topics,len(vocabulary))
# normalise it
for j in range(len(initial_beta)):
    topic = initial_beta[j]
    initial_beta[j] = map(lambda x: x/sum(topic), topic)

corpus_list =[]
alpha_list =[]
for i in range (number_of_corpus):
    corpus, alpha = generate_corpus(100 ,100)
    corpus_list.append(corpus)
    alpha_list.append(alpha)
pickle.dump(corpus_list, open("corpus.p","wb"))
pickle.dump(alpha_list, open("alpha.p","wb"))
pickle.dump(vocabulary, open("vocabulary.p", "wb"))
pickle.dump(initial_beta, open("initial_beta.p", "wb"))