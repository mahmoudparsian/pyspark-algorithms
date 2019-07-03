#!/usr/bin/python
#-----------------------------------------------------
# Apply a sortByKey() transformation to an RDD
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


#=========================================
def create_pair(t3):
    # t3 = (name, city, number)
    name = t3[0]
    #city = t3[1]
    number = int(t3[2])
    return (name, number)
#end-def
#==========================================

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: rdd_transformation_sortbykey.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("rdd_transformation_sortbykey")\
        .getOrCreate()
    #
    print("spark=",  spark)

    #========================================
    # sortByKey() transformation
    #
    # sortByKey(ascending=True, numPartitions=None, keyfunc=<function RDD.<lambda>>)
    #
    # Description:
    # Sorts this RDD, which is assumed to consist of (key, value) pairs.
    #========================================
    # sortBy() transformation
    #
    # sortBy(keyfunc, ascending=True, numPartitions=None)
    #
    # Description:
    # Sorts this RDD by the given keyfunc  
    #========================================
 
    pairs = [(10,"z1"), (1,"z2"), (2,"z3"), (9,"z4"), (3,"z5"), (4,"z6"), (5,"z7"), (6,"z8"), (7,"z9")]
    print("pairs = ", pairs)
    rdd = spark.sparkContext.parallelize(pairs)
    print("rdd.count(): ", rdd.count())
    print("rdd.collect(): ", rdd.collect())

    #-----------------------------------------
    ## Sort by key ascending
    #-----------------------------------------
    sorted_by_key_ascending = rdd.sortByKey(ascending=True)
    print("sorted_by_key_ascending.count(): ", sorted_by_key_ascending.count())
    print("sorted_by_key_ascending.collect(): ", sorted_by_key_ascending.collect())

    #-----------------------------------------
    ## Sort by key descending
    #-----------------------------------------
    sorted_by_key_descending = rdd.sortByKey(ascending=False)
    print("sorted_by_key_descending.count(): ", sorted_by_key_descending.count())
    print("sorted_by_key_descending.collect(): ", sorted_by_key_descending.collect())
    
    
    #-----------------------------------------
    ## Sort by value ascending
    #-----------------------------------------
    sorted_by_value_ascending = rdd.sortBy(lambda x: x[1], ascending=True)
    print("sorted_by_value_ascending.count(): ", sorted_by_value_ascending.count())
    print("sorted_by_value_ascending.collect(): ", sorted_by_value_ascending.collect())

    #-----------------------------------------
    ## Sort by value descending
    #-----------------------------------------
    sorted_by_value_descending = rdd.sortBy(lambda x: x[1], ascending=False)
    print("sorted_by_value_descending.count(): ", sorted_by_value_descending.count())
    print("sorted_by_value_descending.collect(): ", sorted_by_value_descending.collect())
    

     
    # done!
    spark.stop()

