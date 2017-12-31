#This script labels the sentiment of each tweet as either positive/negative or neutral using textblob package

from textblob import TextBlob
import os
import csv

directory = os.listdir('/home/gauravbg/SBU-fall-17/Big_Data/Project/Classified_Data/final')
os.chdir('/home/gauravbg/SBU-fall-17/Big_Data/Project/Classified_Data/final')
write_path = '/home/gauravbg/SBU-fall-17/Big_Data/Project/Classified_Data/final/'
sdg_count = 0
total_count = 0
pos_count = 0
neg_count = 0
neutral_count = 0
for file in directory:
    fn = write_path + file.split('.')[0] + "-loc.csv"
    print(fn)
    all_rows = []
    with open(file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            pred_value = float(row[4])
            tweet = row[1]
            if pred_value == 1:
                sdg_count += 1
                result = TextBlob(tweet)
                score = result.sentiment.polarity
                sentiment_label = ""
                if score >= -0.2 and score <= 0.2:
                    sentiment_label = "Neutral"
                    neutral_count+=1
                elif score > 0.2:
                    sentiment_label = "Positive"
                    pos_count += 1
                else:
                    sentiment_label = "Negative"
                    neg_count += 1
                row[4] = sentiment_label
                all_rows.append(row)
            total_count+=1


    myFile = open(fn, 'w')
    with myFile:
        writer = csv.writer(myFile)
        writer.writerows(all_rows)
print("Stats: ", sdg_count, " : ", total_count, " : ", pos_count, " : ", neg_count, " : ", neutral_count)

