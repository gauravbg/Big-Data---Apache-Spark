from pyspark import SparkConf, SparkContext
import re
from random import *

APP_NAME = "A1P2A"

def chopAndMix(key, in_list):
    result = []
    for each in in_list:
        result.append((each, key))
    return result

if __name__ == "__main__":

   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)

   # -------------------------------------------------------------
   # The code below does word count

   data = [(1, "The horse raced past the barn fell"),
           (2, "The complex houses married and single soldiers and their families"),
           (3, "There is nothing either good or bad, but thinking makes it so"),
           (4, "I burn, I pine, I perish"),
           (5, "Come what come may, time and the hour runs through the roughest day"),
           (6, "Be a yardstick of quality."),
           (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
           (8,
            "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
           (9, "The car raced past the finish line just in time."),
           (10, "Car engines purred and the tires burned.")]

   rdd = sc.parallelize(data)

   word_rdd = rdd.flatMap(lambda x: re.findall(r'\w+', x[1].lower())).map(lambda x: (x, 1)).groupByKey()
   final_rdd = word_rdd.map(lambda x: (x[0], len(list(x[1]))))
   print(final_rdd.collect())



   #-------------------------------------------------------------
   # The code below does set Difference

   data = [('R', ['apple', 'orange', 'pear', 'blueberry']), ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
   data2 = [('R', [x for x in range(50) if random() > 0.5]), ('S', [x for x in range(50) if random() > 0.75])]
   print(data2)
   rdd = sc.parallelize(data2)
   map_out = rdd.flatMap(lambda x: chopAndMix(x[0], x[1])).groupByKey()
   final_rdd = map_out.filter(lambda x: len(x[1]) == 1 and 'S' not in x[1]).map(lambda x: x[0])
   print(final_rdd.collect())