#This is the main Classifier Script.
#It uses Spark Mllib pipeline to train tweet classifier for tweets related to safety, violence and peace.

from pyspark import SparkConf, SparkContext
from pyspark import sql
from nltk.tokenize import TweetTokenizer
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tag.perceptron import PerceptronTagger
import string
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import DoubleType


full_data_file = "/home/gauravbg/SBU-fall-17/Big_Data/Project/Train_Data/train_out.txt"
model_output_file = "/home/gauravbg/SBU-fall-17/Big_Data/Project/model/sdg16.model"

def remove_non_ascii(text):
    if text is None:
        return ""
    init_size = len(text)
    cleaned = re.sub(r'[^\x00-\x7f]',r'', text)
    cleaned_size = len(cleaned)
    if cleaned_size/init_size < 0.3: #ignore tweet if less than 30% after removing non unicode characters
        return ""
    else:
        return cleaned

def remove_stop_words(text):
    stops = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', 'couldn', 'didn', 'doesn', 'hadn', 'hasn', 'haven', 'isn', 'ma', 'mightn', 'mustn', 'needn', 'shan', 'shouldn', 'wasn', 'weren', 'won', 'wouldn']
    relevant_words = []
    tokenizer = TweetTokenizer()
    words = tokenizer.tokenize(text)
    for word in words:
        if word not in stops:
            relevant_words.append(word)
    return " ".join(relevant_words)


def remove_unwanted_tokens(text):

    text = text.lower()
    re_url = re.compile('https?://(www.)?\w+\.\w+(/\w+)*/?')
    text = re_url.sub(' ', text)
    re_tag = re.compile('@(\w+)')
    text = re_tag.sub(' ', text)
    re_punc = re.compile('[%s]' % re.escape(string.punctuation))
    text = re_punc.sub(' ', text)
    re_num = re.compile('(\\d+)')
    text = re_num.sub(' ', text)
    re_alpha_num = re.compile("^[a-z0-9_.]+$")
    wanted = []
    for word in text.split():
        if re_alpha_num.match(word) and len(word) > 2:
            wanted.append(word)
    return " ".join(wanted)

def lemmatize(text):
    lemmatizer = WordNetLemmatizer()
    words = text.split()
    pos_tagged_words = PerceptronTagger().tag(words)
    result = []
    for word in pos_tagged_words:
        if 'v' in word[1].lower():
            lemma = lemmatizer.lemmatize(word[0], pos='v')
        else:
            lemma = lemmatizer.lemmatize(word[0], pos='n')
        result.append(lemma)
    return " ".join(result)

def num_label(text):
    if text == "YES":
        return 1.0
    return 0.0

# def splitIntoCols(line):
#     cols = line.split(",")
#     if len(cols) != 3:
#         return ()
#     id = cols[0].replace('\'', '')
#     id = id.replace('(', '')
#     text = cols[1].replace('\'', '')
#     label = cols[2].replace('\'', '')
#     label = label.replace('(', '')
#     return id, text, label


def splitIntoCols(line):
    cols = line.split(",")
    if len(cols) != 3:
        return ()
    id = cols[0]
    text = cols[1]
    label = cols[2]
    return id, text, label


if __name__ == "__main__":

    # Configure Spark
    APP_NAME = "BIG_DATA_PROJECT"
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sqlContext = sql.SQLContext(sc)

    func_remove_ascii = udf(remove_non_ascii, StringType())
    func_remove_stop_words = udf(remove_stop_words, StringType())
    func_remove_unwanted_tokens = udf(remove_unwanted_tokens, StringType())
    func_lemmatize = udf(lemmatize, StringType())
    func_num_label = udf(num_label, StringType())


    rdd = sc.textFile(full_data_file)
    rdd = rdd.map(splitIntoCols)
    rdd = rdd.filter(lambda x: len(x) == 3)
    df = sqlContext.createDataFrame(rdd, ["Id", "Text", "Annotation"])
    # rdd = sc.binaryFiles('hdfs:/data/large_sample')
    # rdd = sc.binaryFiles('hdfs:/data/small_sample')
    #Username, time, text, location, label
    # rdd_cols = rdd.map(lambda x:splitIntoCols(x))
    # df = sqlContext.createDataFrame(rdd_cols, ["Username", "Time", "Text", "Location", "Label"])

    # df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(full_data_file)
    stripped_df = df.withColumn("Stripped_Text", func_remove_ascii(df["Text"]))
    english_df = stripped_df.filter(stripped_df["Stripped_Text"] != "")
    relevant_words_df = english_df.withColumn("Relevant", func_remove_stop_words(english_df["Stripped_Text"]))
    wanted_words_df = relevant_words_df.withColumn("Wanted", func_remove_unwanted_tokens(relevant_words_df["Relevant"]))
    lemmatized_df = wanted_words_df.withColumn("Lemmatized", func_lemmatize(wanted_words_df["Wanted"]))
    lemmatized_emremoved_df = lemmatized_df.filter(lemmatized_df["Lemmatized"] != "")
    nodup_df = lemmatized_emremoved_df.dropDuplicates(['Lemmatized', 'Annotation']) #Change second param to label
    num_label_df = nodup_df.withColumn("label", func_num_label(nodup_df["Annotation"])) #Change second param to label

    selected_cols_df = num_label_df.select(num_label_df['Lemmatized'], num_label_df['label']) #Change second param to label
    temp_df = selected_cols_df.withColumnRenamed("Lemmatized", "text")
    dataset = temp_df.withColumn("label", temp_df.label.cast(DoubleType()))

    split_dataset = dataset.randomSplit([0.7, 0.3])
    train_dataset = split_dataset[0]
    test_dataset = split_dataset[1]

    #Train the model
    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
    idf = IDF(minDocFreq=3, inputCol="features", outputCol="idf")
    naiveBayes = NaiveBayes()
    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, naiveBayes])

    paramGrid = ParamGridBuilder().addGrid(naiveBayes.smoothing, [0.0, 1.0]).build()

    cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=MulticlassClassificationEvaluator(), numFolds=2)
    cvModel = cv.fit(train_dataset)
    bestModel = cvModel.bestModel
    bestModel.save(model_output_file)

    result = cvModel.transform(test_dataset)
    pred_df = result.select("text", "label", "prediction") #save this df finally

    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    print("Accuracy= ", evaluator.evaluate(result, {evaluator.metricName: "accuracy"}))
