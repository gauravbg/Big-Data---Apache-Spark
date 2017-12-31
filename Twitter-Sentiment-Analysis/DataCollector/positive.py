#This script spwans crawlers to fetch training data i.e pseudo labeled hashtag data

from subprocess import Popen
from subprocess import PIPE
import time
import os
import signal

start_time = time.time()
# positive = ["love", "happiness", "peace", "#nypd", "#lapd", "#fbi", "#cia", "health", "hope", "joy", "dream", "harmony",
#             "cute", "beautiful", "faith", "humanity", "onelove", "firefighters", "veteran",
#             "heroes", "marine", "vet", "thank", "grateful"]

positive = ["love", "happiness", "peace", "#nypd", "#lapd"]

for keyword in positive:
    print("-->Keyword: " + keyword)
    command = 'scrapy crawl TweetScraper -a query=' + keyword
    p = Popen(command, stdout=PIPE, shell=True)
    time.sleep(60)
    os.kill(p.pid, signal.SIGINT)
    # call(command, shell=True)

print("--- %s seconds ---" % (time.time() - start_time))
