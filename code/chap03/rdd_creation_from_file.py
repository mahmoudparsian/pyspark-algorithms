#!/usr/bin/python
#-----------------------------------------------------
# Create an RDD from a File
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    a File
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
#
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
def create_pair(record):
    tokens = record.split(",")
    key = tokens[0]
    value = tokens[1]
    return (key, int(value))
#end-def
#======================================


if __name__ == '__main__':

    if len(sys.argv) != 2:  
        print("Usage: rdd_creation_from_file.py <file>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("rdd_creation_from_file")\
        .getOrCreate()
    #
    print("spark=",  spark)
    
    # read name of input file
    input_path = sys.argv[1]
    print("input path : ", input_path)
    debug_file(input_path)
    

    #=====================================
    # Create an RDD[String] from a given input file
    #=====================================
    #
    rdd = spark.sparkContext.textFile(input_path)
    print("rdd =",  rdd)
    print("rdd.count = ",  rdd.count())
    print("rdd.collect() = ",  rdd.collect())
    
    #=====================================
    # Create  pairs  RDD[String, Integer] from a RDD[String]
    #=====================================
    pairs = rdd.map(create_pair)
    print("pairs =",  pairs)
    print("pairs.count = ",  pairs.count())
    print("pairs.collect() = ",  pairs.collect())
    #
    #=====================================
    # filter some elements: keep the (k, v) pairs where v > 300
    #=====================================    
    filtered = pairs.filter(lambda kv : kv[1] > 300)
    print("filtered =",  filtered)
    print("filtered.count = ",  filtered.count())
    print("filtered.collect() = ",  filtered.collect())    
    
    # done!
    spark.stop()
