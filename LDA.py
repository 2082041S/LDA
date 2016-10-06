import numpy

def generate_word_topic_distribution():
	beta = []
	topic_sum=[]
	for topic in range (k):
		beta.append([])
		sum = 0
		for word in range (vocab_length):
			if (vocab_length/k*topic <= word and word <vocab_length/k*(topic+1)):
				randint =numpy.random.random()
				beta[topic].append(randint)
				sum += randint
			else:
				beta[topic].append(0)
		topic_sum.append(sum) 
	for topic in range (k):
		for word in range (vocab_length):
			beta[topic][word]= beta[topic][word]/topic_sum[topic]
	return beta;

# Generate Document
k = 5 # topics
vocabulary = ["president", "vote", "corruption", "government",
		"books", "exam", "lecture","student",
		"bishop","knight","king","pawn",
		"actor", "movie", "TV", "cinema",
		"goal","football","goalkeeper","penalty"]
vocab_length = len(vocabulary)
beta = generate_word_topic_distribution()

print beta

alpha = 0.1 # concentration of topics per document 
N = 100 # documents
