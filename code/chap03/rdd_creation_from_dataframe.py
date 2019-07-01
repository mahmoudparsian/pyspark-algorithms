#!/usr/bin/python
#-----------------------------------------------------
# Create an RDD From a DataFrame 
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
from pyspark.sql import Row


#

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: rdd_creation_from_dataframe.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("rdd_creation_from_dataframe")\
        .getOrCreate()
    #
    print("spark=",  spark)

    #===================================================================
    # Create a DataFrame from a list of values with default column names
    #===================================================================
    list_of_pairs = [("alex", 1), ("alex", 5), ("bob", 2), ("bob", 40), ("jane", 60), ("mary", 700), ("adel", 800) ]
    print("list_of_pairs=",  list_of_pairs)
    #
    # create a DataFrame from a collection
    # Distribute a local Python collection to form a DataFrame
    df = spark.createDataFrame(list_of_pairs)
    print("df = ",  df)
    print("df.count = ",  df.count())
    print("df.collect() = ",  df.collect())
    df.show()
    df.printSchema()
     
    #===================================================================
    # Convert an existing DataFrame to an RDD
    #===================================================================
    rdd = df.rdd
    print("rdd = ",  df)
    print("rdd.count = ",  rdd.count())
    print("rdd.collect() = ",  rdd.collect())
    
          
    # done!
    spark.stop()

