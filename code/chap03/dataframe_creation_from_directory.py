#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame from  CSV files in a given directory
# Input: a directory
#------------------------------------------------------
# Input Parameters:
#    a directory (which contains .csv files)
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
import os

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

    if len(sys.argv) != 2:  
        print("Usage: dataframe_creation_from_directory.py <dir>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_from_directory")\
        .getOrCreate()
    #
    print("spark=",  spark)

    # read name of input file
    input_dir = sys.argv[1]
    print("input_dir : ", input_dir)
    dir_listing = os.listdir(input_dir)  
    print("dir_listing = ",  dir_listing)
    

    #=====================================
    # Create a DataFrame from a given directory,
    # which contains CSV files. Read only the 
    # files ending with ".csv"
    #=====================================
    
    # The following example reads all CSV files 
    # (files ending with ".csv") in a given directory
    df = spark\
          .read\
          .format("csv")\
          .option("header","false")\
          .option("inferSchema", "true")\
          .load(input_dir+"/*.csv")
    #
    print("df = " , df.collect())
    #
    df.show()    
    #
    df.printSchema()


       
    # done!
    spark.stop()

