#!/usr/bin/python
#-----------------------------------------------------
# This is a word count in PySpark.
# The goal is to show how "word count" works.
# Here we write transformations in a shorthand!
#
# RULES:
#   RULE-1:
#        Here I introduce the RDD.filter() transformation
#        to ignore the words if their length is less than 3.
#        This is implemented by:
#            .filter(lambda word : len(word) > THRESHOLD_WORD_LENGTH)
#   RULE-2:
#        If the total frequency of any unique word is less
#        than 2, then ignore that word from the final output
#        This is implemented by:
#            .filter(lambda (k, v) : v > THRESHOLD_FREQUENCY)
#
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

#=====================================
def debug_file(input_path):
    # Opening a file in python for reading is easy:
    f = open(input_path, 'r')

    # To get everything in the file, just use read()
    file_contents = f.read()

    #And to print the contents, just do:
    print ("file_contents = \n" + file_contents)

    # Don't forget to close the file when you're done.
    f.close()
#end-def
#=====================================


if __name__ == '__main__':

    print("len(sys.argv) = ", len(sys.argv))
    print("script: sys.argv[0] = ", sys.argv[0])
    print("p1:     sys.argv[1] = ", sys.argv[1])
    print("p2:     sys.argv[2] = ", sys.argv[2])
    print("p3:     sys.argv[3] = ", sys.argv[3])
    #
    if len(sys.argv) != 4:
        print("Usage: spark-submit word_count_driver_with_filter_and_threshold.py <input-path> <THRESHOLD_WORD_LENGTH> <THRESHOLD_FREQUENCY>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Word-Count-App")\
        .getOrCreate()

    print("script: {}".format(sys.argv[0]))
    #
    #  sys.argv[1] is the first parameter
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))
    debug_file(input_path)
    
    THRESHOLD_WORD_LENGTH = int(sys.argv[2])
    print("THRESHOLD_WORD_LENGTH = ", THRESHOLD_WORD_LENGTH)
    #
    THRESHOLD_FREQUENCY = int(sys.argv[3]) 
    print("THRESHOLD_FREQUENCY = ", THRESHOLD_FREQUENCY)

    # create frequencies as RDD<unique-word, frequency>
    # filter(): if len(word) < 3, then drop that word
    frequencies = spark.sparkContext.textFile(input_path)\
        .filter(lambda line: len(line) > 0)\
        .flatMap(lambda line: line.lower().split(" "))\
        .filter(lambda word : len(word) > THRESHOLD_WORD_LENGTH)\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)
    # 
    print("frequencies.count(): ", frequencies.count())
    print("frequencies.collect(): ", frequencies.collect())

    filtered = frequencies.filter(lambda (k, v) : v > THRESHOLD_FREQUENCY)
    print("filtered.count(): ", filtered.count())
    print("filtered.collect(): ", filtered.collect())
    
    # done!
    spark.stop()
