#!/usr/bin/python
#-----------------------------------------------------
# Create an RDD from a .gz files (which 
# may have any number of files in it)
# Input: .gz File(s) 
#------------------------------------------------------
# Input Parameters:
#    Zipped File
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql import Row

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
#

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: datasource_gzip_reader.py <csv-file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("datasource_gzip_reader")\
        .getOrCreate()
    #
    print("spark=",  spark)

    # read name of input file(s)
    gz_input_path = sys.argv[1]
    print("gz_input_path : ", gz_input_path)
    #debug_file(gz_input_path)
    

    #=====================================
    # Create an RDD from a given .gz file(s)
    #=====================================
    gzip_rdd = spark.sparkContext.textFile(gz_input_path)
    #
    print("gzip_rdd = " , gzip_rdd)
    #
    print("gzip_rdd.count() = " , gzip_rdd.count())
    #
    print("gzip_rdd.collect() = " , gzip_rdd.collect())

       
    # done!
    spark.stop()

