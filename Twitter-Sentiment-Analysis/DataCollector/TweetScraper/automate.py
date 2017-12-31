from subprocess import Popen
from subprocess import PIPE
import time
import os
import signal
import csv

input_file = open("users.csv", "r")
csv_reader = csv.reader(input_file)

start_time = time.time()


def get_location(line):
    city = line[5]
    if (city == "NO_DATA"):
        city = "TBD"
    county = line[6]
    if (county == "NO_DATA"):
        county = "TBD"
    state = line[7]
    if (state == "NO_DATA"):
        state = "TBD"
    location = "-".join([city, county, state])
    return location


next(csv_reader)
count = 0
start_check_point = 0
prev_check_point = 0
batch_size = 100
for line in csv_reader:
    if line[4] == "YES":
        loc = get_location(line)
        if count < start_check_point:
            continue
        username = line[1]

        fn = "--".join([username, loc])

        command = 'scrapy crawl TweetScraper -s SAVE_TWEET_PATH=sdg_16/test_data_15_seconds/' + fn + ' -a query=@' + username
        p = Popen(command, stdout=PIPE, shell=True)

        count += 1
        print("Crawling " + username + " tweets..")
        if count == (prev_check_point + batch_size):
            time.sleep(15)
            prev_check_point = count
            # if count>= 10:
            #     break

print("--- %s seconds ---" % (time.time() - start_time))

input_file.close()
