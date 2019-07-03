#!/usr/bin/python
#-----------------------------------------------------
# Apply a mapPartitions() transformation to an RDD
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
"""
Spark's mapPartitions()
According to Spark API: mapPartitions(func)    transformation is 
similar to map(), but runs separately on each partition (block) 
of the RDD, so func must be of type Iterator<T> => Iterator<U> 
when running on an RDD of type T.

The mapPartitions() transformation should be used when you want 
to extract some condensed information (such as finding the 
minimum and maximum of numbers) from each partition. For example, 
if you want to find the minimum and maximum of all numbers in your 
input, then using map() can be pretty inefficient, since you will 
be generating tons of intermediate (K,V) pairs, but the bottom line 
is you just want to find two numbers: the minimum and maximum of 
all numbers in your input. Another example can be if you want to 
find top-10 (or bottom-10) for your input, then mapPartitions() 
can work very well: find the top-10 (or bottom-10) per partition, 
then find the top-10 (or bottom-10) for all partitions: this way 
you are limiting emitting too many intermediate (K,V) pairs.
"""

from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 


#=========================================
def debug_a_partition(iterator):
    print("==begin-partition=")
    for x in iterator:
        print(x)
    #end-for
    print("==end-partition=")
#end-def
#==========================================
# iterator : a pointer to a single partition
# (min, max) will be returned for a single partition
#
def minmax(iterator):
    first_time = 0
    for x in iterator:
        if (first_time == 0):
            min = x;
            max = x;
            first_time = 1
        else:
            if x > max:
                max = x
            if x < min:
                min = x
        #end-if
    #end-for
    return (min, max)
#end-def
#==========================================

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: rdd_transformation_mappartitions.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("rdd_transformation_mappartitions")\
        .getOrCreate()
    #
    print("spark=",  spark)

    #========================================
    # mapPartitions() transformation
    #
    # source_rdd.mapPartitions(function) --> target_rdd
    #
    # mapPartitions() is a 1-to-1 transformation:
    # Return a new RDD by applying a function to each partition of this RDD;
    # maps a partition into a single element of the target RDD
    #
    # mapPartitions(f, preservesPartitioning=False)[source]
    # Return a new RDD by applying a function to each partition of this RDD.
    #
    #========================================
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    print("numbers = ", numbers)
    # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
  
    # create an RDD with 3 partitions
    rdd = spark.sparkContext.parallelize(numbers, 3)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())
    print("rdd.getNumPartitions() = ", rdd.getNumPartitions())
    rdd.foreachPartition(debug_a_partition)

    # Find Minimum and Maximum
    # Use mapPartitions() and find the minimum and maximum 
    # from each partition.  To make it a cleaner solution, 
    # we define a python function to return the minimum and 
    # maximum for a given iteration.
    minmax_rdd = rdd.mapPartitions(minmax)
    print("minmax_rdd = ", minmax_rdd)
    print("minmax_rdd.count() = ", minmax_rdd.count())
    print("minmax_rdd.collect() = ", minmax_rdd.collect())


    minmax_list = minmax_rdd.collect()
    print("minmax_list = ", minmax_list)
    print("min(minmax_list) = ", min(minmax_list))
    print("max(minmax_list) = ", max(minmax_list))

    
    # done!
    spark.stop()

