from subprocess import Popen
from subprocess import PIPE
import time
import os
import signal

start_time = time.time()
keywords = ["#faith", "#humanity", "#onelove", "#firefighter", "#veteran", "#heroes", "#marine", "#vet", "#thank",
            "#grateful", "#injustice", "#corruption", "#discrimination", "#racism", "#war", "#abuse", "#NotOneMore",
            "#guncontrol", "#gunsense", "#love", "#happiness", "#peace", "#nypd", "#lapd", "#fbi", "#cia", "#police",
            "#health", "#wealth", "#dream", "#harmony", "#gunviolence", "#protest", "#2ndamendment", "#metoo",
            "#secondamendment"]

for keyword in keywords:
    print("-->Keyword: " + keyword)
    command = 'scrapy crawl TweetScraper -s SAVE_TWEET_PATH=~/volatile/' + keyword + ' -a query=' + keyword
    p = Popen(command, stdout=PIPE, shell=True)
    with open("process_details.txt", "a") as p_file:
        p_file.write(str(keyword) + " || " + str(p.pid))
        p_file.write("\n")

print("--- %s seconds ---" % (time.time() - start_time))
