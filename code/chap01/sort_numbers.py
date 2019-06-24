#!/usr/bin/python
#-----------------------------------------------------
# SORT NUMBERS:
#
# 1. Create an RDD from a text File,
#    containing numbers separated by a space 
#
# 2. Sort all numbers
#------------------------------------------------------
# Input Parameters:
#   A text File containing numbers
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

#=======================================================
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
#=======================================================


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: sort_numbers <file>", file=sys.stderr)
        exit(-1)

    #------------------------------------------
    # create an instance of SparkSession object
    #------------------------------------------
    spark = SparkSession\
        .builder\
        .appName("sort_numbers")\
        .getOrCreate()
    #
    print("spark=",  spark)
    
    # read name of input file
    input_path = sys.argv[1]
    print("input path : ", input_path)
    debug_file(input_path)
    
    
    #------------------------------------------------
    # create an RDD[String]
    # each element of the RDD will be a  
    # single record as a String
    #------------------------------------------------
    records = spark.sparkContext.textFile(input_path)
    print("records =",  records)
    print("records.count = ",  records.count())
    print("records.collect() = ",  records.collect())
    
    #------------------------------------------------
    # 1. split a record into numbers and flatten it
    # 2. create (number, 1) pairs
    # 3. sort by numbers (as a key)
    #------------------------------------------------
    sorted_count = records\
        .flatMap(lambda rec: rec.split(' '))\
        .map(lambda n: (int(n), 1))\
        .sortByKey()
        
    # NOTE: This is just a demo on how to bring 
    # all the sorted data back to a single node.
    # In reality, we wouldn't want to collect all 
    # the data to the driver node.
    output = sorted_count.collect()
    #
    print("sorted numbers:")
    for (num, unitcount) in output:
        print(num)

    spark.stop()
