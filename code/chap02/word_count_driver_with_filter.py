#!/usr/bin/python
#-----------------------------------------------------
# This is a word count in PySpark.
# The goal is to show how "word count" works.
# Here we write transformations in a shorthand!
#
# RULES:
#
#   RULE-1:
#        Here I introduce the RDD.filter() transformation
#        to ignore the words if their length is less than 3.
#        This is implemented by:
#            .filter(lambda word : len(word) > 2)
#   RULE-2:
#        If the total frequency of any unique word is less
#        than 2, then ignore that word from the final output
#        This is implemented by:
#            .filter(lambda (k, v) : v > 1)
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

    if len(sys.argv) != 2:  
        print("Usage: word_count_driver.py <input-path>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Word-Count-App")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))
    debug_file(input_path)

    # create frequencies as RDD<unique-word, frequency>
    # Rule-1: filter(): if len(word) < 3, then 
    # drop that word
    frequencies = spark.sparkContext.textFile(input_path)\
        .filter(lambda line: len(line) > 0)\
        .flatMap(lambda line: line.lower().split(" "))\
        .filter(lambda word : len(word) > 2)\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)
    # 
    print("frequencies.count(): ", frequencies.count())
    print("frequencies.collect(): ", frequencies.collect())

    # Rule-2: filter(): if frequency of a word is > 1, 
    # then keep that word
    filtered = frequencies.filter(lambda (k, v) : v > 1)
    print("filtered.count(): ", filtered.count())
    print("filtered.collect(): ", filtered.collect())
    
    # done!
    spark.stop()
