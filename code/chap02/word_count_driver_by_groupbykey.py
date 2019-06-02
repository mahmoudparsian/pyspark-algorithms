#!/usr/bin/python
#-----------------------------------------------------
# This is a word count in PySpark.
# The goal is to show how "word count" works.
#
# Uses groupByKey() to show how it is used.
#
# Overall reduceByKey() performs and scales-out 
# better than groupByKey()
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

if __name__ == '__main__':

    if len(sys.argv) != 2:  
        print("Usage: word_count_driver_by_groupbykey.py <input-file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Word-Count-App")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))

    # read input and create an RDD<String>
    records = spark.sparkContext.textFile(input_path) 
    print("records.count(): ", records.count())
    print("records.collect(): ", records.collect())
    
    # Filter out non-empty lines from the loaded file 
    non_empty_records = records.filter(lambda x: len(x) > 0)
    print("non_empty_records.count(): ", non_empty_records.count())
    print("non_empty_records.collect(): ", non_empty_records.collect())

    # convert all words to lowercase and flatten it to words
    words = non_empty_records.flatMap(lambda line: line.lower().split(" ")) 
    print("words.count(): ", words.count())
    print("words.collect(): ", words.collect())

    # create a pair of (word, 1) for all words
    pairs =  words.map(lambda word: (word, 1)) 
    print("pairs.count(): ", pairs.count())
    print("pairs.collect(): ", pairs.collect())

    # group values by each unique word
    grouped_by_unique_words = pairs.groupByKey() 
    print("grouped_by_unique_words.count(): ", grouped_by_unique_words.count())
    print("grouped_by_unique_words.collect(): ", grouped_by_unique_words.collect())

    # aggregate the frequencies of each unique word
    # counts is an iterable of numbers
    frequencies = grouped_by_unique_words.mapValues(lambda counts : sum(counts)) 
    print("frequencies.count(): ", frequencies.count())
    print("frequencies.collect(): ", frequencies.collect())

    # done!
    spark.stop()
