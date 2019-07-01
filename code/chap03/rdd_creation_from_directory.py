#!/usr/bin/python
#-----------------------------------------------------
# Create an RDD from a Directory,
# which has a set of files in it.
# Input: a directory
#------------------------------------------------------
# Input Parameters:
#    a directory
#-------------------------------------------------------
#
# $ cd sample_dir/
# $ ls -l
# -rw-r--r--  1 mparsian  897801646  54 Nov 11 18:59 file1.txt
# -rw-r--r--  1 mparsian  897801646  90 Nov 11 19:00 file2.txt
#
# $ cat file1.txt
# record 1 of file1
# record 2 of file1
# record 3 of file1
#
# $ cat file2.txt
# record 1 of file2
# record 2 of file2
# record 3 of file2
# record 4 of file2
# record 5 of file2
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession
import os
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
        print("Usage: rdd_creation_from_directory.py <dir>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("rdd_creation_from_directory")\
        .getOrCreate()
    #
    print("spark=",  spark)
    
    # read name of input directory
    dir_path = sys.argv[1]
    print("dir path : ", dir_path)
    dir_listing = os.listdir(dir_path)  
    print("dir_listing = ",  dir_listing)
    

    #=====================================
    # Create an RDD[String] from a given input directory
    #=====================================
    #
    rdd = spark.sparkContext.textFile(dir_path)
    print("rdd =",  rdd)
    print("rdd.count = ",  rdd.count())
    print("rdd.collect() = ",  rdd.collect())
    
    #
    #=====================================
    # filter some elements: keep the records, 
    # which contain "3"
    #=====================================    
    filtered = rdd.filter(lambda record : record.find('3') != -1)
    print("filtered =",  filtered)
    print("filtered.count = ",  filtered.count())
    print("filtered.collect() = ",  filtered.collect())    
    
    # done!
    spark.stop()
