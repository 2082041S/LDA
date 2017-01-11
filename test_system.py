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

    def test_vocabulary(self):
        vocabulary = pickle.load(open("vocabulary.p","rb"))
        for word in vocabulary:
            is_right_format = word_has_right_format(word)
            if is_right_format:
                self.failIf(is_right_format,"Vocabulary word does not have right format: "+ word)

        print "Vocabulary tests passed"

    def test_corpus(self):
        corpus_list = pickle.load(open("corpus.p", "rb"))
        for corpus_name in corpus_list:
            corpus = corpus_list[corpus_name]
            for document_name in corpus:
                document = corpus[document_name]
                is_right_format = document_has_right_format(document_name)
                self.failIf(is_right_format, "Document name within corpus " +corpus_name +
                            " does not have right format: " + document_name)
                for word in document:
                    is_right_format = word_has_right_format(word)
                    self.failIf(is_right_format, "Word within corpus " +corpus_name +
                                " within document "+ document_name +" does not have right format: " + word)
                    intensity = document[word]
                    self.failIf(not isfloat(intensity), "Word " + word + " within corpus " +corpus_name +
                                " within document "+ document_name +" does not have right intensity: " + str(intensity))

        print "Corpus tests passed"

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestInputData)
    unittest.TextTestRunner(verbosity=2).run(suite)