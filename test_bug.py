import cPickle as pickle
import pdb

from lda import VariationalLDA
import numpy as np

corpus_list_dict = pickle.load(open("corpus.p", "rb"))
number_of_topics = 500
vocabulary = pickle.load(open("vocabulary.p","rb"))
word_index={}
for pos in range(len(vocabulary)):
    word_index[vocabulary[pos]] = pos
for name in corpus_list_dict:
    if name == "Beer_multibeers_12_fullscan1_ms":
        lda_object = VariationalLDA(corpus=corpus_list_dict[name], K=number_of_topics, eta=0.1, alpha=1, word_index= word_index)

convergence_number = 0.01
it = 0
beta_diff = 0
new_beta = np.zeros((number_of_topics, len(vocabulary)))
while it < 1000:
    if it == 0:
        lda_object.run_vb(initialise=True)
    else:
        lda_object.run_vb(initialise=False)
    old_beta= new_beta
    new_beta = lda_object.beta_matrix
    if np.isnan(new_beta[0][0]):
        print "First element of old beta", old_beta[0][0]
        print "First elemnet of alpha", lda_object.alpha[0]
        print "First element of phi matrix", lda_object.phi_matrix[0][0]
        print "First element of gamma", lda_object.gamma_matrix[0][0]
        pdb.set_trace()
    beta_diff = np.sum(abs(np.subtract(new_beta, old_beta)))
    print "iteration: ", it, "beta difference: ", beta_diff
    it += 1

print "Finished converging Beta " + str(beta_diff)
