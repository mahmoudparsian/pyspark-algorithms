#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame from a Python dictionary
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
    #    print("Usage: dataframe_creation_from_dictionary.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_from_dictionary")\
        .getOrCreate()
    #
    print("spark=",  spark)


    #===================================================================
    # Create a DataFrame from a list of values with default column names
    #===================================================================
    mydict = {'A': '1', 'B':'2', 'D':'8', 'E': '99'}
    print("mydict=",  mydict)
    #
    # create a DataFrame from a dictionary
    # Distribute a local Python collection to form a DataFrame
    column_names = ["key", "value"]
    df = spark.createDataFrame(mydict.items(), column_names)
    print("df = ",  df)
    print("df.count = ",  df.count())
    print("df.collect() = ",  df.collect())
    df.show()
    df.printSchema()
    
       
    # done!
    spark.stop()
