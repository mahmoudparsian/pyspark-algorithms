#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame and DROP a COLUMN
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
    #    print("Usage: dataframe_drop_column.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_drop_column")\
        .getOrCreate()


    # Dropping a Column from a DataFrame
    # The following examples show how to drop an
    # existing column from a  DataFrame. 


    # Let's first create a DataFrame with 3 columns:
    data = [(100, "a", 1.0), (200, "b", 2.0), (300, "c", 3.0), (400, "d", 4.0)]
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
    # |200|    b|   2.0|
    # |300|    c|   3.0|
    # |400|    d|   4.0|
    # +---+-----+------+

    #============================================
    # DataFrame.drop(*cols)
    #
    # Description:
    #    Returns a new DataFrame that drops the specified 
    #    column. This is a no-op if schema doesn't contain 
    #    the given column name(s).
    #
    # Parameters:	cols - a string name of the column to 
    #                      drop, or a Column to drop, or a 
    #                      list of string name of the columns 
    #                      to drop.
    #============================================

    # Drop Column "scale"
    df.drop('scale').show() 

    # Drop Column "code"
    df.drop('code').show() 

    # done!
    spark.stop()
