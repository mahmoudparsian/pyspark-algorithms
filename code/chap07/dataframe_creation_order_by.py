#!/usr/bin/python
#-----------------------------------------------------
# Sorting DataFrame:
# Create a DataFrame and call "orderBy()" (for sorting) 
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
    #    print("Usage: dataframe_creation_order_by.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_order_by")\
        .getOrCreate()


    # Sort DataFrame
    #
    # How do you sort a DataFrame based on column(s)?
    # We can use the `orderBy()` operation on DataFrame 
    # to get sorted output based on some column. 
    #
    # The `orderBy()` operation take two arguments.
    #
    #  orderBy(*cols, **kwargs)
    #  Returns a new DataFrame sorted by the specified column(s).
    #
    # Parameters:
    #   cols - list of Column or column names to sort by.
    #   ascending - boolean or list of boolean (default True). 
    #   Sort ascending vs. descending. Specify list for multiple 
    #   sort orders. If a list is specified, length of the list 
    #   must equal length of the cols.

    # Lets's create a DataFrame and then apply `orderBy()`.
    data = [ ('A', 8), ('B', 3), ('A', 4), ('B', 2), ('Z', 7) ]
    print("data=", data)
    # [('A', 8), ('B', 3), ('A', 4), ('B', 2), ('Z', 7)]
    df = spark.createDataFrame(data, ["id", "value"])
    df.show()
    # +---+-----+
    # | id|value|
    # +---+-----+
    # |  A|    8|
    # |  B|    3|
    # |  A|    4|
    # |  B|    2|
    # |  Z|    7|
    # +---+-----+

    # The following examples show how to use `orderBy()`
    # in ascending mode for a given DataFrame (sorted by
    # the "id" column):
    df.orderBy(df.id).show()
    # +---+-----+
    # | id|value|
    # +---+-----+
    # |  A|    4|
    # |  A|    8|
    # |  B|    2|
    # |  B|    3|
    # |  Z|    7|
    # +---+-----+

    # You may use `desc()` to sort your data in descending mode:
    df.orderBy(df.id.desc()).show()
    # +---+-----+
    # | id|value|
    # +---+-----+
    # |  Z|    7|
    # |  B|    3|
    # |  B|    2|
    # |  A|    8|
    # |  A|    4|
    # +---+-----+

    # You may sort your numeric data:
    df.orderBy(df.value).show()
    # +---+-----+
    # | id|value|
    # +---+-----+
    # |  B|    2|
    # |  B|    3|
    # |  A|    4|
    # |  Z|    7|
    # |  A|    8|
    # +---+-----+

    # You may sort your numeric data in descending order:
    df.orderBy(df.value.desc()).show()
    # +---+-----+
    # | id|value|
    # +---+-----+
    # |  A|    8|
    # |  Z|    7|
    # |  A|    4|
    # |  B|    3|
    # |  B|    2|
    # +---+-----+

    # Your may sort your data using multiple columns in ascending order:
    df.orderBy(df.id, df.value).show()
    # +---+-----+
    # | id|value|
    # +---+-----+
    # |  A|    4|
    # |  A|    8|
    # |  B|    2|
    # |  B|    3|
    # |  Z|    7|
    # +---+-----+

    # Your may sort your data using multiple columns in mix of 
    # ascending (the "id" column) and descending (the "value"
    # column) order:
    df.orderBy(df.id, df.value.desc()).show()
    # +---+-----+
    # | id|value|
    # +---+-----+
    # |  A|    8|
    # |  A|    4|
    # |  B|    3|
    # |  B|    2|
    # |  Z|    7|
    # +---+-----+

    # done!
    spark.stop()
