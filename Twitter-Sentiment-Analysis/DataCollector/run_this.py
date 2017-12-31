#This script spwans crawlers to fetch training data i.e pseudo labeled hashtag data

from subprocess import Popen
from subprocess import PIPE
import time
import os
import signal

start_time = time.time()
keywords = ["cute", "beautiful", "faith", "humanity", "onelove", "firefighters", "veteran",
            "heroes", "marine", "vet", "thank", "grateful", "injustice", "corruption", "discrimination", "racialdiscrimination",
            "intolerance", "war", "abuse", "#NotOneMore", "#guncontrol", "#gunsense"]

for keyword in keywords:
    print("-->Keyword: " + keyword)
    command = 'scrapy crawl TweetScraper -s SAVE_TWEET_PATH=C:/school/big_data_tweets_data/test/' + keyword + ' -a query=' + keyword
    p = Popen(command, stdout=PIPE, shell=True)
    with open("process_details.txt", "a") as p_file:
        p_file.write(str(keyword) + " || " + str(p.pid))
        p_file.write("\n")

print("--- %s seconds ---" % (time.time() - start_time))
