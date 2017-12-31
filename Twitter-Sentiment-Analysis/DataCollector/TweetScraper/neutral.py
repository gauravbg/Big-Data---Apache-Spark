from subprocess import Popen
from subprocess import PIPE
import time
import os
import signal

start_time = time.time()
keywords = ["#sports", "#NFL", "#science", "#techtrends", "#hollywood", "#datascience", "#blackfriday", "#travel",
            "#music", "#GoT"]

for keyword in keywords:
    print("-->Keyword: " + keyword)
    command = 'scrapy crawl TweetScraper -s SAVE_TWEET_PATH=~/neutral/' + keyword + ' -a query=' + keyword
    p = Popen(command, stdout=PIPE, shell=True)
    with open("process_details.txt", "a") as p_file:
        p_file.write(str(keyword) + " || " + str(p.pid))
        p_file.write("\n")

print("--- %s seconds ---" % (time.time() - start_time))
