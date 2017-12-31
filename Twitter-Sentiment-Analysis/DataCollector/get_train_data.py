#This Spark Program will create file with required data in the correct format from all the tweets collected from hashtags. This data is used fo training

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from json import loads
import re
import datetime
import string
import io
import csv


def list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip()  # remove extra newline


def extract_required_info(path, data):
    path_list = path.split("/")
    tweet_id = path_list[-1]
    label_cat = path_list[-3]
    label = ""
    if (label_cat == "volatile"):
        label = "YES"
    elif label_cat == "neutral":
        label = "NO"

    json_data = loads(data)
    text = json_data['text']
    # text = text.encode('utf-8', 'ignore')
    # text = text.decode()
    cleaned = re.sub(r'[^\x00-\x7f]', r'', text)
    re_punc = re.compile('[%s]' % re.escape(string.punctuation))
    text = re_punc.sub(' ', cleaned)
    # re.sub("\n", " ", text)
    date = json_data['datetime']
    date = date.split(" ")[0]
    # date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    separator_token = "-|-%%-|-"
    # res = separator_token.join([str(tweet_id), str(text), label])

    return str(tweet_id), str(text), label


if __name__ == '__main__':
    conf = SparkConf().setAppName("myFirstApp").setMaster("local")
    sc = SparkContext(conf=conf)
    rdd = sc.wholeTextFiles("C:/school/big data/train/*/*")
    trans_rdd = rdd.map(lambda x: extract_required_info(x[0], x[1]))
    trans_rdd.map(list_to_csv_str)
    # print(rdd.take(10))
    trans_rdd.saveAsTextFile("text/")
