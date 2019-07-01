#!/usr/bin/python
#-----------------------------------------------------
# Create an RDD from a Collection
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
    #    print("Usage: rdd_creation_from_collection.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("rdd_creation_from_collection")\
        .getOrCreate()
    #
    print("spark=",  spark)


    #=====================================
    # Create an RDD from a list of values
    #=====================================
    list_of_strings = ["alex", "bob", "jane", "mary", "adel" ]
    print("list_of_strings=",  list_of_strings)
    #
    # create an RDD from a collection
    # Distribute a local Python collection to form an RDD
    rdd1 = spark.sparkContext.parallelize(list_of_strings)
    print("rdd1=",  rdd1)
    print("rdd1.count=",  rdd1.count())
    print("rdd1.collect()=",  rdd1.collect())
    
    #=====================================
    # Create an RDD from a list of pairs
    #=====================================
    list_of_pairs = [("alex", 1), ("alex", 3), ("alex", 9), ("alex", 10), ("bob", 4), ("bob", 8)]
    print("list_of_pairs = ",  list_of_pairs)
    #
    # create an RDD from a collection
    rdd2 = spark.sparkContext.parallelize(list_of_pairs)
    print("rdd2 = ",  rdd2)
    print("rdd2.count = ",  rdd2.count())
    print("rdd2.collect() = ",  rdd2.collect())
    
    # add values per key
    rdd2_added = rdd2.reduceByKey(lambda x, y: x+y)
    print("rdd2_added = ",  rdd2_added)
    print("rdd2_added.count = ",  rdd2_added.count())
    print("rdd2_added.collect() = ",  rdd2_added.collect())
  
    # group values per key
    rdd2_grouped = rdd2.groupByKey()
    print("rdd2_grouped = ",  rdd2_added)
    print("rdd2_grouped.count = ",  rdd2_grouped.count())
    print("rdd2_grouped.collect() = ",  rdd2_grouped.collect())
    print("rdd2_grouped.collect() = (as a list) = ",  rdd2_grouped.mapValues(lambda values: list(values)).collect())

    #================================
    # create an RDD from a dictionary
    #================================
    d = {"key1":"value1","key2":"value2","key3":"value3"} 
    print("d = ",  d)
    print("d.items()= ",  d.items())
    rdd_from_dict = spark.sparkContext.parallelize(d.items())
    print("rdd_from_dict = ",  rdd_from_dict)
    print("rdd_from_dict.collect() = ",  rdd_from_dict.collect())
    print("rdd_from_dict.count = ",  rdd_from_dict.count())
    

    # done!
    spark.stop()
