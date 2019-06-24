#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame and ADD NEW COLUMNS 
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
from pyspark.sql.functions import lit
from pyspark.sql.functions import exp
from pyspark.sql.functions import rand
from pyspark.sql.functions import col

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_creation_add_columns.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_add_columns")\
        .getOrCreate()


    # Adding Columns to a DataFrame
    # The following examples show how to add new
    # columns to an existing DataFrames. For each 
    # example, we will do something different.


    # Let's first create a DataFrame with 3 columns:
    data = [(100, "a", 3.0), (300, "b", 5.0)]
    columns = ("x1", "x2", "x3")
    print("data=", data)
    # [(100, 'a', 3.0), (300, 'b', 5.0)]
    
    # create a new DataFrame
    df = spark.createDataFrame(data , columns)   
    df.show() 
    # +---+---+----+
    # | x1| x2|  x3|
    # +---+---+----+
    # |100|  a| 3.0|
    # |300|  b| 5.0|
    # +---+---+----+


    # Next we add a new column (using `DataFrame.withColumn()`
    # function) labeled as `x4`, and set the default value as 
    # literal zero. We use lit(), a literal function
    df_with_x4 = df.withColumn("x4", lit(0)) 
    df_with_x4.show()
    # +---+---+----+---+
    # | x1| x2|  x3| x4|
    # +---+---+----+---+
    # |100|  a| 3.0|  0|
    # |300|  b| 5.0|  0|
    # +---+---+----+---+


    # Spark enable us to transform an existing column
    # (by using its column values) to a new column: The 
    # following example computes the exponential of the 
    # column "x3" value as a new column "x5":
    # creates a new column "x5" and initialize it to exp("x3")    
    df_with_x5 = df_with_x4.withColumn("x5", exp("x3"))
    df_with_x5.show()
    # +---+---+---+---+------------------+
    # | x1| x2| x3| x4|                x5|
    # +---+---+---+---+------------------+
    # |100|  a|3.0|  0|20.085536923187668|
    # |300|  b|5.0|  0| 148.4131591025766|
    # +---+---+---+---+------------------+


    # You may perform the `join()` operation between 
    # two DataFrames and hence add new additional columns.
    # The following example joins two DataFrames (named 
    # as `df_with_x5` and `other_df`) and creates a new 
    # DataFrame as `df_with_x6`.
    other_data = [(100, "foo1"), (100, "foo2"), (200, "foo")]
    other_df = spark.createDataFrame(other_data, ("k", "v"))
    df_with_x6 = (df_with_x5\
        .join(other_df, col("x1") == col("k"), "leftouter")\
        .drop("k")\
        .withColumnRenamed("v", "x6"))

    df_with_x6.show()
    # +---+---+---+---+------------------+----+
    # | x1| x2| x3| x4|                x5|  x6|
    # +---+---+---+---+------------------+----+
    # |100|  a|3.0|  0|20.085536923187668|foo1|
    # |100|  a|3.0|  0|20.085536923187668|foo2|
    # |300|  b|5.0|  0| 148.4131591025766|null|
    # +---+---+---+---+------------------+----+


    # You may also use other functions (imported 
    # from `pyspark.sql.functions`) such as 
    # `rand()` to create new columns. The `rand()` 
    # function generates a random value with 
    # independent and identically distributed samples 
    # from `[0.00, 1.00]`.
    df_with_x7 = df_with_x6.withColumn("x7", rand())
    df_with_x7.show()
    # +---+---+---+---+------------------+----+-------------------+
    # | x1| x2| x3| x4|                x5|  x6|                 x7|
    # +---+---+---+---+------------------+----+-------------------+
    # |100|  a|3.0|  0|20.085536923187668|foo1| 0.5037931478944555|
    # |100|  a|3.0|  0|20.085536923187668|foo2|0.04816326889227163|
    # |300|  b|5.0|  0| 148.4131591025766|null| 0.5374597910281964|
    # +---+---+---+---+------------------+----+-------------------+


    # Using the `rand()` function will create different 
    # values (since it is a randome number generator) 
    # every time you run it:
    df_with_x8 = df_with_x6.withColumn("x8", rand())
    df_with_x8.show()
    # +---+---+---+---+------------------+----+------------------+
    # | x1| x2| x3| x4|                x5|  x6|                x8|
    # +---+---+---+---+------------------+----+------------------+
    # |100|  a|3.0|  0|20.085536923187668|foo1|0.7777162777548193|
    # |100|  a|3.0|  0|20.085536923187668|foo2|0.3090201186619811|
    # |300|  b|5.0|  0| 148.4131591025766|null|0.9458253592984243|
    # +---+---+---+---+------------------+----+------------------+


    # done!
    spark.stop()
