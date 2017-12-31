#This Program will create file with required data in the correct format for classifying the tweets using the trained model. (Used for local work)

import os
import json
import re
import string
import csv

input_path = "C:/school/big data/test_sample"
csv_file_path = "C:/school/big data/test_out/test_out.txt"


def extract_required_values(json_data, path):
    # print(path)

    path_list = path.split("/")
    tweet_id = ""
    tweet_id = path_list[-1]
    username_loc = path_list[-2]

    user_tokens = username_loc.split("--")
    username = user_tokens[0]
    loc = user_tokens[1]

    text = json_data['text']
    cleaned = re.sub(r'[^\x00-\x7f]', r'', text)
    re_punc = re.compile('[%s]' % re.escape(string.punctuation))
    text = re_punc.sub(' ', cleaned)
    text = text.replace("\n", " ")
    text = text.replace("\r", " ")
    date = json_data['datetime']
    date = date.split(" ")[0]

    row = ','.join([tweet_id, text, username, date, loc])
    return row


def process_input():
    csv_file = open(csv_file_path, "a")
    for dir in os.listdir(input_path):
        # username
        username_folders_path = os.path.join(input_path, dir).replace("\\", "/")
        print(username_folders_path)
        for addr in os.listdir(username_folders_path):
            # individual file
            addr = os.path.join(username_folders_path, addr).replace("\\", "/")
            with open(addr, "r") as f:
                for line in f:
                    json_data = json.loads(line)
                    row = extract_required_values(json_data, addr)
                    csv_file.write(row)
                    csv_file.write("\n")

    csv_file.close()


if __name__ == '__main__':
    process_input()
