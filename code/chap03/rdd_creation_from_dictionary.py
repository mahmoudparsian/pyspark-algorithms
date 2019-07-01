#!/usr/bin/python
#-----------------------------------------------------
# Create an RDD[key, value] from a Python dictionary
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
#

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: rdd_creation_from_dictionary.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("rdd_creation_from_dictionary")\
        .getOrCreate()
    #
    print("spark=",  spark)


    #===================================================================
    # Create a DataFrame from a list of values with default column names
    #===================================================================
    mydict = {'A': '1', 'B':'2', 'D':'8', 'E': '99'}
    print("mydict=",  mydict)
    #
    # create an RDD[key, value] from a dictionary
    # Distribute a local Python collection to form an RDD
    # column_names = ["key", "value"]
    rdd = spark.sparkContext.parallelize(mydict.items())
    print("rdd = ",  rdd)
    print("rdd.count = ",  rdd.count())
    print("rdd.collect() = ",  rdd.collect())
    
       
    # done!
    spark.stop()
