import re
from xml.etree import ElementTree as ET
from pyspark import SparkConf, SparkContext

APP_NAME = "A1P2B"


def parseXml(text):
    unwanted = [
        ('&nbsp;', ''),
        ('&acirc;', ''),
        ('&', '')]
    result = list()
    date = ''
    content = ''
    parser = ET.XMLParser(encoding="utf-8")
    text = text.encode("ascii", "ignore")
    for before, after in unwanted:
        text = text.replace(before, after)
    try:
        root = ET.fromstring(text)
    except Exception as ex:
        result.append(("ERROR",
                       "Consulting"))  # (Consulting, (ERROR, count) will give the number of xml files that could not be parsed
        return result

    for i, child in enumerate(root):
        if child.tag == 'date':
            date_arr = child.text.split(',')[1:]
            date = "{}-{}".format(date_arr[1], date_arr[0])
        elif child.tag == 'post':
            content = child.text
            result.append((date, content))
    return result


def getindustries(record):
    date_tag = record[0]
    all_tokens = list()
    for industry in all_industries.value:
        count = 0
        count = len(re.findall(r'(?<!\S)' + re.escape(industry) + r'(?!\S)', record[1], re.IGNORECASE))
        #         words = record[1].lower().split()
        #         for word in words:
        #             if word == industry:
        #                 count += 1
        if count > 0:
            all_tokens.append((industry, (date_tag, count)))
    return all_tokens


def reduceDateWise(item):
    industry = item[0]
    my_dict = dict()
    for value in list(item[1]):
        fulldate = value[0]
        my_dict[fulldate] = my_dict.get(fulldate, 0) + value[1]
    result = tuple(tuple((key, value)) for key, value in my_dict.items())
    return (industry, result)


if __name__ == "__main__":

   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)

   #Give the correct path of the input folder below. * Will fetch all files in the folder
   rdd = sc.wholeTextFiles('/home/gauravbg/SBU-fall-17/Big Data/blogs/*')
   industries_rdd = rdd.map(lambda x: x[0].split(".")[3].lower()).distinct()
   print(industries_rdd.collect())
   all_industries = sc.broadcast(industries_rdd.collect())

   rdd = sc.wholeTextFiles('/home/gauravbg/SBU-fall-17/Big Data/blogs/*')
   xml_parsed_rdd = rdd.flatMap(lambda x: parseXml(x[1]))
   industry_date_rdd = xml_parsed_rdd.flatMap(lambda x: getindustries(x)).groupByKey()
   final_rdd = industry_date_rdd.map(lambda x: reduceDateWise(x))
   print(final_rdd.collect())