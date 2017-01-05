import csv
from os import walk
import cPickle as pickle

def get_beer_files():
    dir = "C:/simon_rogers/POS_beer/"
    files = {}
    for i in range(1, 19):
        linked_files = []
        linked_name = "Beer_multibeers_" + str(i) + "_fullscan1_ms"
        for j in range(1, 3):
            file_content = []
            file_name = "Beer_multibeers_" + str(i) + "_fullscan1_ms" + str(j) + ".csv"
            with open(dir + file_name, "rb") as f:
                reader = csv.reader(f)
                for row in reader:
                    file_content.append(row)
            linked_files.append(file_content)
        files[linked_name] = linked_files
    return files


def add_urine_files(files):
    dir = "C:/simon_rogers/POS_urine/"
    file_names = []
    for (dirpath, dirnames, filenames) in walk(dir):
        file_names.extend(filenames)
        break

    csv_files=[]
    for file in file_names:
        if file.endswith(".csv"):
            csv_files.append(file)

    #ms1 is followed by ms2 in csv_files
    for i in range (0, len(csv_files), 2):
        linked_files = []
        linked_name = csv_files[i][:-5]
        for j in range(2):
            file_content = []
            file_name = csv_files[i+j]
            with open(dir + file_name, "rb") as f:
                reader = csv.reader(f)
                for row in reader:
                    file_content.append(row)
            linked_files.append(file_content)
        files[linked_name] = linked_files


def add_word_to_bin(bin_dict, bin, mass):
    if bin in bin_dict:
        bin_dict[bin] += mass
    else:
        bin_dict[bin] = mass


def add_intensity_to_doc(bin_dict, bin, intensity):
    if bin in bin_dict:
        bin_dict[bin] += intensity
    else:
        bin_dict[bin] = intensity


def main():
    files = get_beer_files()
    add_urine_files(files)
    delta = 0.05
    corpus_dict = {}
    vocabulary = {}
    fragment_bin_centre = {}
    loss_bin_centre = {}
    count = 0
    list_of_corpus_info = []
    for linked_file in files.values():
        ms1_file = linked_file[0]
        ms2_file = linked_file[1]
        corpus_info = {}

        for i in range (len(ms1_file)):
            row = ms1_file[i]
            if i != 0:
                doc_id = row[0]
                doc_rt = row[4]
                doc_mass = row[5]
                document_name = str(float(doc_mass)) + "_" + str(float(doc_rt))
                corpus_info[doc_id] = document_name

        for i in range(len(ms2_file)):
            row = ms2_file[i]
            if i != 0:
                fragment_parent = row[2]
                parent_name = corpus_info[fragment_parent]
                fragment_mass = float(row[5])
                fragment_intensity = float(row[6])
                parent_mass = float(parent_name.split("_")[0])
                loss_mass = parent_mass - fragment_mass

                if fragment_intensity > 0.01:
                    fragment_bin = int(fragment_mass / delta)
                    add_word_to_bin(fragment_bin_centre, fragment_bin, fragment_mass)
                    if 10 < loss_mass < 200:
                        loss_bin = int(loss_mass / delta)
                        add_word_to_bin(loss_bin_centre, loss_bin, loss_mass)


        list_of_corpus_info.append(corpus_info)

    for file_name in files:
        linked_file = files[file_name]
        ms1_file = linked_file[0]
        ms2_file = linked_file[1]
        corpus_info = {}
        corpus = {}
        for i in range (len(ms1_file)):
            row = ms1_file[i]
            if i != 0:
                doc_id = row[0]
                doc_rt = row[4]
                doc_mass = row[5]
                document_name = str(float(doc_mass)) + "_" + str(float(doc_rt))
                fragment_bin_dict = {}
                loss_bin_dict = {}
                corpus_info[doc_id] = [document_name, fragment_bin_dict, loss_bin_dict]

        for i in range(len(ms2_file)):
            row = ms2_file[i]
            if i != 0:
                fragment_parent = row[2]
                parent_name = corpus_info[fragment_parent][0]

                if parent_name not in corpus:
                    corpus[parent_name] = {}

                fragment_mass = float(row[5])
                fragment_intensity = float(row[6])
                parent_mass = float(parent_name.split("_")[0])
                loss_mass = parent_mass - fragment_mass
                fragment_bin_dict = corpus_info[fragment_parent][1]
                loss_bin_dict = corpus_info[fragment_parent][2]
                if fragment_intensity > 0.01:
                    fragment_bin = int(fragment_mass / delta)
                    word = "fragment_" + str(fragment_bin_centre[fragment_bin])
                    vocabulary[word] = True

                    if word in corpus[parent_name]:
                        corpus[parent_name][word] += fragment_intensity * 1000
                    else:
                        corpus[parent_name][word] = fragment_intensity * 1000
                    if 10 < loss_mass < 200:
                        loss_bin = int(loss_mass / delta)
                        word = "loss_" + str(loss_bin_centre[loss_bin])
                        vocabulary[word] =True
                        

                        if word in corpus[parent_name]:
                            corpus[parent_name][word] += fragment_intensity * 1000
                        else:
                            corpus[parent_name][word] = fragment_intensity * 1000

        corpus_dict[file_name] = corpus

    print len(vocabulary)
    print corpus_dict.keys()
    print corpus_dict.values()[0].values()[0]
    pickle.dump(corpus_dict, open("corpus.p","wb"))
    pickle.dump(vocabulary.keys(), open("vocabulary.p", "wb"))
if __name__ == '__main__':
    main()
