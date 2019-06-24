#!/usr/bin/python
#-----------------------------------------------------
# 1. Create an RDD from a CSV File 
#    from Input as a CSV File of the 
#    following format:
#
#    <name><,><city><,><age>
#
# 2. Find average of age per city.
#------------------------------------------------------
# Input Parameters:
#   A CSV File
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
    #
    tokens = record.split(",")
    #
    # name = tokens[0]
    city = tokens[1]
    age = int(tokens[2])
    #
    return (city, (age, 1))
#end-def
#======================================
# a = (sum1, count1) = (a[0], a[1])
# b = (sum2, count2) = (b[0], b[1])
# sum = sum1+sum2
# count = count1+count2
# return (sum, count)
#
def add_pairs(a, b):
    #
    sum = a[0]+b[0]
    count = a[1]+b[1]
    #
    return (sum, count)
#end-def
#======================================


if __name__ == '__main__':

    if len(sys.argv) != 2:  
        print("Usage: rdd_creation_from_csv.py <file>", file=sys.stderr)
        exit(-1)

    #------------------------------------------
    # create an instance of SparkSession object
    #------------------------------------------
    spark = SparkSession\
        .builder\
        .appName("rdd_creation_from_csv")\
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
    # Create RDD[String, (Integer, Integer)] 
    # as RDD[name, (age, 1)] From an RDD[String]
    #=====================================
    pairs = rdd.map(create_pair)
    print("pairs =",  pairs)
    print("pairs.count = ",  pairs.count())
    print("pairs.collect() = ",  pairs.collect())
    #
    #=====================================
    # Add up all ages per city and count of them
    #=====================================    
    sum_and_count = pairs.reduceByKey(lambda a, b : add_pairs(a, b))
    print("sum_and_count =",  sum_and_count)
    print("sum_and_count.count = ",  sum_and_count.count())
    print("sum_and_count.collect() = ",  sum_and_count.collect())    
    
    #=====================================
    # sum_and_count = [(city1, (sum1, count1)), (city2, (sum2, count2)), ...]
    # Find average per city
    #===================================== 
    # pair = (pair[0], pair[1]) = (sum_of_ages, count_of_ages)   
    average_per_city = sum_and_count.mapValues(lambda pair : float(pair[0]) / float(pair[1]) )
    print("average_per_city =",  average_per_city)
    print("average_per_city.count = ",  average_per_city.count())
    print("average_per_city.collect() = ",  average_per_city.collect())    
        
    # done!
    spark.stop()
