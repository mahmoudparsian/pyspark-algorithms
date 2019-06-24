#---------------------
# Chapter 1
# @author Mahmoud Parsian
#
# word_count_driver.py
#---------------------
from __future__ import print_function
import sys
from pyspark.sql import SparkSession


if __name__ == '__main__':
    
    if len(sys.argv) != 2:
        print("Usage: word_count_with_params <file>", file=sys.stderr)
        exit(-1)
    
    # create an instance of a SparkSession
    spark = SparkSession\
        .builder\
        .appName("word_count_with_params")\
        .getOrCreate()

    # Execute word count for an input file
    #
    #   sys.argv[0] is the name of the script.
    #   sys.argv[1] is the first parameter
    input_path = sys.argv[1] # input file
    print("input_path: {}".format(input_path))
    
    # read input and create an RDD<String>
    records_rdd = spark.read.textFile(input_path)
    print("records_rdd.count(): ", records_rdd.count())
    
    # convert all words to lowercase and flatten it to words
    words_rdd = records_rdd.flatMap(lambda line: line.lower().split(" "))
    print("words_rdd.count(): ", words_rdd.count())
    
    # create a piar of (word, 1) for all words
    pairs_rdd =  words_rdd.map(lambda word: (word, 1))
    print("pairs_rdd.count(): ", pairs_rdd.count())
    
    # aggregate the frequencies of each unique word
    frequencies_rdd = pairs_rdd.reduceByKey(lambda a, b: a + b)
    print("frequencies_rdd.collect(): ", frequencies_rdd.collect())
    
    # keep only the words with frequency of over 30 and len(k) > 2
    filtered_rdd = frequencies_rdd.filter(lambda (k,v) : v > 30 and len(k) > 2)
    print("filtered_rdd.collect(): ", filtered_rdd.collect())
    
    # done!
    spark.stop()