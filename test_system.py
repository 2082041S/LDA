import unittest
import cPickle as pickle

def isfloat(value):
  try:
    float(value)
    return True
  except:
    return False

def word_has_right_format(word):
    is_right_format = False
    if "_" not in word:
        is_right_format = True
    else:
        split_list = word.split("_")
        if len(split_list) != 2:
            is_right_format = True
        else:
            first_part = split_list[0]
            second_part = split_list[1]
            if first_part != "loss" and first_part != "fragment":
                is_right_format = True
            elif not isfloat(second_part):
                is_right_format = True

    return is_right_format


def document_has_right_format(document):
    is_right_format = False
    if "_" not in document:
        is_right_format = True
    else:
        split_list = document.split("_")
        if len(split_list) != 2:
            is_right_format = True
        else:
            first_part = split_list[0]
            second_part = split_list[1]
            if not isfloat(first_part):
                is_right_format = True
            elif not isfloat(second_part):
                is_right_format = True

    return is_right_format


class TestInputData(unittest.TestCase):

    def setUp(self):
        self.vocabulary = pickle.load(open("vocabulary.p","rb"))
        self.corpus_list = pickle.load(open("corpus.p", "rb"))
    def test_vocabulary_word_format(self):
        for word in self.vocabulary:
            is_right_format = word_has_right_format(word)
            if is_right_format:
                self.failIf(is_right_format,"Vocabulary word does not have right format: "+ word)

    def test_vocabulary_for_duplicity(self):
        contains_duplicates = len(self.vocabulary) != len(set(self.vocabulary))   
        self.assertFalse(contains_duplicates,"Vocabulary contains duplicates")
    def test_corpus(self):
        vocab = {}
        for corpus_name in self.corpus_list:
            words_added = 0
            corpus = self.corpus_list[corpus_name]
            for document_name in corpus:
                document = corpus[document_name]
                is_right_format = document_has_right_format(document_name)
                self.failIf(is_right_format, "Document name within corpus " +corpus_name +
                            " does not have right format: " + document_name)
                for word in document:
                    if word not in vocab:
                        vocab[word] = True
                        words_added +=1
                    is_right_format = word_has_right_format(word)
                    self.failIf(is_right_format, "Word within corpus " +corpus_name +
                                " within document "+ document_name +" does not have right format: " + word)
                    intensity = document[word]
                    self.failIf(not isfloat(intensity), "Word " + word + " within corpus " +corpus_name +
                                " within document "+ document_name +" does not have right intensity: " + str(intensity))
            print words_added,


class TestOutputData(unittest.TestCase):
    def setUp(self):
        self.file_list = pickle.load(open("/results/corpus.p","rb")) 

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestInputData)
    unittest.TextTestRunner(verbosity=2).run(suite)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestOutputData)
    unittest.TextTestRunner(verbosity=2).run(suite)
