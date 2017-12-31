#This script pwans scrapy crawlers to fetch users historical data

from subprocess import Popen
from subprocess import PIPE
import time
import os
import signal
import csv

input_file = open("users.csv")
csv_reader = csv.reduce(input_file, "r")

start_time = time.time()

for line in csv_reader[:100]:
    username = line[1]

    command = 'scrapy crawl TweetScraper -s SAVE_TWEET_PATH=sdg_16/test_data/' + username + ' -a query=@' + username
    p = Popen(command, stdout=PIPE, shell=True)

print("--- %s seconds ---" % (time.time() - start_time))

input_file.close()
