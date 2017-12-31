#This Spark Program will create file with required data from all the tweets collected for classifying the tweets

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from json import loads
import datetime


def extract_required_info(path, data):
    path_list = path.split("/")
    tweet_id = path_list[-1]
    username_loc = path_list[-2]

    user_tokens = username_loc.split("--")
    username = user_tokens[0]
    loc = user_tokens[1]
    json_data = loads(data)
    text = json_data['text']
    text = text.encode('ascii', 'ignore')
    text.replace('\r\n', ' ')
    date = json_data['datetime']
    date = date.split(" ")[0]

    separator_token = "-|-%%-|-"
    res = separator_token.join([str(tweet_id), str(text.decode()), str(username), str(date), str(loc)])

    return res


if __name__ == '__main__':
    conf = SparkConf().setAppName("myFirstApp").setMaster("local")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    rdd = sc.wholeTextFiles("C:/school/big data/sample/*/*.txt")
    rdd = rdd.map(lambda x: extract_required_info(x[0], x[1]))
    # print(rdd.take(10))
    rdd.saveAsTextFile("text/")
    # result = rdd.collect().coalesce(1).saveAsTextFile("text/")
    # print(rdd.count())
    # df = sqlContext.createDataFrame(rdd, ["USER_ID", "TEXT", "DATE", "LABEL"])
    # df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save('C:/school/big_data_tweets_data/TweetScraper/csv_out_new.csv')
