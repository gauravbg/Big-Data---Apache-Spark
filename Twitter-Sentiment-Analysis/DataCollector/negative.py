#This script spwans crawlers to fetch training data i.e pseudo labeled hashtag data

from subprocess import call
import time

start_time = time.time()
negative = ["gun", "violence", "protest", "2ndamendment", "riot", "#metoo", "hatecrime", "felony", "misconduct",
            "secondamendment"]

for keyword in negative:
    command = 'scrapy crawl TweetScraper -a query=' + keyword
    call(command, shell=True)

print("--- %s seconds ---" % (time.time() - start_time))
