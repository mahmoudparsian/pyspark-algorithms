#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame and DROP duplicate rows (elements)
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
    #    print("Usage: dataframe_drop_duplicates.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_drop_duplicates")\
        .getOrCreate()


    # Dropping duplicate rows from a DataFrame
    # The following examples show how to drop 
    # duplicate rows from an existing DataFrame. 


    # Let's first create a DataFrame with 3 columns:
    data = \
       [(100, "a", 1.0),\
        (100, "a", 1.0),\
        (200, "b", 2.0),\
        (300, "c", 3.0),\
        (300, "c", 3.0),\
        (400, "d", 4.0)]
        
    print("data = ", data)
    #
    columns = ("id", "code", "scale")
    print("columns = ", columns)
    
    # create a new DataFrame
    df = spark.createDataFrame(data , columns)   
    df.show() 
    # +---+-----+------+
    # | id| code| scale|
    # +---+-----+------+
    # |100|    a|   1.0|
    # |100|    a|   1.0|
    # |200|    b|   2.0|
    # |300|    c|   3.0|
    # |300|    c|   3.0|
    # |400|    d|   4.0|
    # +---+-----+------+

    #============================================
    # DataFrame.dropDuplicates(subset=None)
    #
    # Description:
    #     Return a new DataFrame with duplicate rows removed, 
    #     optionally only considering certain columns. For a 
    #     static batch DataFrame, it just drops duplicate rows. 
    #     For a streaming DataFrame, it will keep all data across 
    #     triggers as intermediate state to drop duplicates rows. 
    #     You can use withWatermark() to limit how late the 
    #     duplicate data can be and system will accordingly limit 
    #     the state. In addition, too late data older than watermark 
    #     will be dropped to avoid any possibility of duplicates.
    # 
    # drop_duplicates() is an alias for dropDuplicates().
    #============================================

    # Drop duplicate rows
    df.dropDuplicates().show() 



    # done!
    spark.stop()
