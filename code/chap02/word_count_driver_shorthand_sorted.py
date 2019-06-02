#!/usr/bin/python
#-----------------------------------------------------
# This is a word count in PySpark.
# The goal is to show how "word count" works.
# Here we write transformations in a shorthand!
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
        print("Usage: word_count_driver_shorthand_sorted.py <input-file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Word-Count-App")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))

    # create frequencies as RDD<unique-word, frequency>
    frequencies = spark.sparkContext.textFile(input_path)\
        .filter(lambda line: len(line) > 0)\
        .flatMap(lambda line: line.lower().split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)
    # 
    print("frequencies.count(): ", frequencies.count())
    print("frequencies.collect(): ", frequencies.collect())
    
    #-----------------------------------------
    ## Sort frequencies by key (unique words):
    #-----------------------------------------
    sorted_by_key = frequencies.sortBy(lambda (k,v): k)
    print("sorted_by_key.count(): ", sorted_by_key.count())
    print("sorted_by_key.collect(): ", sorted_by_key.collect())

    #-----------------------------------------
    ## Sort frequencies by value (counts):
    #-----------------------------------------
    sorted_by_value = frequencies.sortBy(lambda (k,v): v)
    print("sorted_by_value.count(): ", sorted_by_value.count())
    print("sorted_by_value.collect(): ", sorted_by_value.collect())
    
    # done!
    spark.stop()
