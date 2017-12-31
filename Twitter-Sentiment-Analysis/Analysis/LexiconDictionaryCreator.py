#This script creates a dictionary of sentiment lexicons by parsing various open source datasets

import os
from collections import defaultdict
import xlrd


LEXICONS_FOLDER = "Lexicons"
current_file = os.path.abspath(os.path.dirname(__file__))
lexicons_path = os.path.join(current_file, LEXICONS_FOLDER)

lexicon_map = defaultdict(list)


files_list = ["AFINN-96.txt", "AFINN-111.txt", "inquireraugmented.xls", "SentiWordNet.txt", "subjclueslen.tff", "NRC-Emoticon-unigrams.txt", "NRC-Emotion-Lexicon-Wordlevel.txt"]
for file in files_list:
        if file == "AFINN-96.txt" or file == "AFINN-111.txt":
            f = open(os.path.join(lexicons_path, file), "r")
            lines = f.readlines()
            for line in lines:
                word = line.split("\t")[0]
                score = line.split("\t")[1].strip()
                lexicon_map[word].append(score)
        elif file == "subjclueslen.tff":
            f = open(os.path.join(lexicons_path, file), "r")
            lines = f.readlines()
            for line in lines:
                try:
                    word = line.split(" ")[2].split("=")[1].strip()
                    score = line.split(" ")[5].split("=")[1].strip()
                    lexicon_map[word].append(score)
                except IndexError as ex:
                    print(ex)
        elif file == "SentiWordNet.txt":
            f = open(os.path.join(lexicons_path, file), "r")
            lines = f.readlines()
            data_started = False
            for line in lines:
                if data_started:
                    cols = line.split("\t")
                    words = cols[4].split(" ")
                    for word in words:
                        lexicon_map[word.split("#")[0]].append(("P", cols[2]))
                        lexicon_map[word.split("#")[0]].append(("N", cols[3]))
                if "SynsetTerms" in line and "Gloss" in line and "PosScore" in line:
                    # Data starts from here
                    data_started = True
        elif file == "inquireraugmented.xls":
            workbook = xlrd.open_workbook(os.path.join(lexicons_path, file))
            sheet = workbook.sheet_by_index(0)
            row_count = sheet.nrows
            for i in range(2, row_count):
                word = sheet.cell(i, 0).value
                if sheet.cell(i, 2).value != xlrd.empty_cell.value:
                    lexicon_map[word].append("P")
                if sheet.cell(i, 3).value != xlrd.empty_cell.value:
                    lexicon_map[word].append("N")
        elif file == "NRC-Emotion-Lexicon-Wordlevel.txt":
            f = open(os.path.join(lexicons_path, file), "r")
            lines = f.readlines()
            positives = ["anticipation", "joy", "positive"]
            negatives = ["anger", "disgust", "fear", "negative", "sadness", "trust"]
            for i, line in enumerate(lines):
                if i==0: continue
                tokens = line.split("\t")
                word = tokens[0]
                emotion = tokens[1]
                val = int(tokens[2].strip())
                if emotion in positives and val == 1:
                    lexicon_map[word].append("P")
                if emotion in negatives and val == 1:
                    lexicon_map[word].append("N")
        elif file == "NRC-Emoticon-unigrams.txt":
            f = open(os.path.join(lexicons_path, file), "r")
            lines = f.readlines()
            for line in lines:
                tokens = line.split("\t")
                word = tokens[0]
        f.close()

print(len(lexicon_map.keys()))

for key, value in lexicon_map.items():
    print(key, value)