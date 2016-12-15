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


def add_word_to_bin(bin_dict, bin, mass, intensity):
    if bin in bin_dict:
        bin_dict[bin][0] += mass
        bin_dict[bin][1] += intensity
    else:
        bin_dict[bin] = [mass, intensity]


def main():
    files = get_beer_files()
    add_urine_files(files)
    delta = 0.05
    corpus_list = []
    vocabulary=[]
    count = 0
    for linked_file in files.values():
        ms1_file = linked_file[0]
        ms2_file = linked_file[1]
        corpus = {}
        doc_info = {}

        for i in range (len(ms1_file)):
            row = ms1_file[i]
            if i != 0:
                doc_id = row[0]
                doc_rt = row[4]
                doc_mass = row[5]
                document_name = str(float(doc_mass)) + "_" + str(float(doc_rt))
                fragment_bin_dict = {}
                loss_bin_dict = {}
                doc_info[doc_id] = [document_name, fragment_bin_dict, loss_bin_dict]
                corpus[document_name]={}

        for i in range(len(ms2_file)):
            row = ms2_file[i]
            loss_bin = -1
            fragment_bin = -1
            if i != 0:
                fragment_parent = row[2]
                parent_name = doc_info[fragment_parent][0]
                fragment_mass = float(row[5])
                fragment_intensity = float(row[6])
                parent_mass = float(parent_name.split("_")[0])
                loss_mass = parent_mass - fragment_mass
                fragment_bin_dict = doc_info[fragment_parent][1]
                loss_bin_dict = doc_info[fragment_parent][2]
                if fragment_intensity > 0.01:
                    fragment_bin = int(fragment_mass / delta)
                    add_word_to_bin(fragment_bin_dict, fragment_bin, fragment_mass, fragment_intensity)
                    if 10 < loss_mass < 200:
                        loss_bin = int(loss_mass / delta)
                        add_word_to_bin(loss_bin_dict, loss_bin, loss_mass, fragment_intensity)

        for doc in doc_info.values():
            document_name = doc[0]
            fragment_bin_dict = doc[1]

            for fragment_bin in fragment_bin_dict.values():
                fragment_name = "fragment_" + str(fragment_bin[0])
                corpus[document_name][fragment_name] = fragment_bin[1] * 1000
                vocabulary.append(fragment_name)

            loss_bin_dict = doc[2]

            for loss_bin in loss_bin_dict.values():
                loss_name = "loss_" + str(loss_bin[0])
                corpus[document_name][loss_name] = loss_bin[1] * 1000
                vocabulary.append(loss_name)

        corpus_list.append(corpus)
    print len(vocabulary)
    pickle.dump(corpus_list, open("corpus.p","wb"))
    pickle.dump(vocabulary, open("vocabulary.p", "wb"))
if __name__ == '__main__':
    main()
