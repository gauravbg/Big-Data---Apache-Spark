#This Program will create file with required data in the correct format from all the tweets collected from hashtags. This data is used fo training. (Used for local work)

import os
import json
import re
import string
import csv

input_path = "C:/school/big data/train_new"
csv_file_path = "C:/school/big data/train_out/train_out_new.txt"


def extract_required_values(json_data, path):
    # print(path)

    path_list = path.split("/")
    tweet_id = ""
    tweet_id = path_list[-1]
    label_cat = path_list[-3]
    label = ""
    if label_cat == "volatile":
        label = "YES"
    elif label_cat == "neutral":
        label = "NO"

    text = json_data['text']
    cleaned = re.sub(r'[^\x00-\x7f]', r'', text)
    re_punc = re.compile('[%s]' % re.escape(string.punctuation))
    text = re_punc.sub(' ', cleaned)
    text = text.replace("\n", " ")
    text = text.replace("\r", " ")
    row = []

    row = ','.join([tweet_id, text, label])
    return row


def process_input():
    csv_file = open(csv_file_path, "a")
    for dir in os.listdir(input_path):
        # neutral volatile
        neutral_volatile_folder_path = os.path.join(input_path, dir).replace("\\", "/")
        # print(neutral_volatile_folder_path)
        for d in os.listdir(neutral_volatile_folder_path):
            # Keyword folder
            keyword_folders_path = os.path.join(neutral_volatile_folder_path, d).replace("\\", "/")
            # print(keyword_folders_path)
            for addr in os.listdir(keyword_folders_path):
                # individual file
                addr = os.path.join(keyword_folders_path, addr).replace("\\", "/")
                with open(addr, "r") as f:
                    for line in f:
                        json_data = json.loads(line)
                        row = extract_required_values(json_data, addr)
                        csv_file.write(row)
                        csv_file.write("\n")

    csv_file.close()


if __name__ == '__main__':
    process_input()
