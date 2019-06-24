#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame 
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

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_creation_from_collections.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_from_collections")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    # input_path = sys.argv[1]  
    # print("input_path: {}".format(input_path))


    # DataFrames Creation from Collections

    # DataFrames can be created from Python collections
    # (such as list of strings, list of tuples, ...).
    # For example, the following code segment creates a
    # DataFrame from a given list of pairs, where each pair
    # is an instance of (String, Integer):

    data = [('k1', 2), ('k1', 3), ('k1', 5), ('k2', 7), ('k2', 9), ('k3', 8)]
    print("data=", data)
    # [('k1', 2), ('k1', 3), ('k1', 5), ('k2', 7), ('k2', 9), ('k3', 8)]

    # create a DataFrame from a collection:
    df = spark.createDataFrame(data)
    df.show()
    # +---+---+
    # | _1| _2|
    # +---+---+
    # | k1|  2|
    # | k1|  3|
    # | k1|  5|
    # | k2|  7|
    # | k2|  9|
    # | k3|  8|
    # +---+---+

    # As you can see, the column names have default 
    # names (`_1` for the first column, `_2` for the 
    # second column, ...).  When creating a DataFrame 
    # from a collection, you may define (i.e., impose)
    # your own column names as:
    df2 = spark.createDataFrame(data, ['my_key', 'my_value'])
    df2.show()
    # +------+--------+
    # |my_key|my_value|
    # +------+--------+
    # |    k1|       2|
    # |    k1|       3|
    # |    k1|       5|
    # |    k2|       7|
    # |    k2|       9|
    # |    k3|       8|
    # +------+--------+

    # Once a DataFrame is created, you may create
    # a logical table view and then issue SQL queries 
    # against it:
    # Register a DataFrame as a table called "my_table"
    df2.createOrReplaceTempView("my_table")
    # Use a registered table in your SQL queries
    df3 = spark.sql("SELECT * FROM my_table WHERE my_value > 6")
    df3.show()
    # +------+--------+
    # |my_key|my_value|
    # +------+--------+
    # |    k2|       7|
    # |    k2|       9|
    # |    k3|       8|
    # +------+--------+


    # You may group values by key:
    df4 = spark.sql("SELECT my_key, count(*) as size FROM my_table GROUP BY my_key")
    df4.show()
    # +------+----+
    # |my_key|size|
    # +------+----+
    # |    k2|   2|
    # |    k1|   3|
    # |    k3|   1|
    # +------+----+


    # done!
    spark.stop()
