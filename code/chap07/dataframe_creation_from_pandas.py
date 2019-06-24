#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame from Pandas' DataFrame
# Input: NONE
#
# Pandas is a software library written for 
# the Python programming language for data 
# manipulation and analysis. In particular, 
# it offers data structures and operations for 
# manipulating numerical tables and time series.
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
import pandas as pd

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_creation_from_pandas.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_from_pandas")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    # input_path = sys.argv[1]  
    # print("input_path: {}".format(input_path))


    # DataFrames Creation from Pandas DataFrame

    # DataFrames can be created from Pandas DataFrames
    panda_dataframe = pd.DataFrame(
        data = {
            'integers': [2, 5, 7, 8, 9],
            'floats': [1.2, -2.0, 1.5, 2.7, 3.6],
            'int_arrays': [[6], [1, 2], [3, 4, 5], [6, 7, 8, 9], [10, 11, 12]]
        }
    )
    print("panda_dataframe = \n", panda_dataframe)
    
    
    # create a Spark DataFrame from a Pandas DataFrame:
    spark_df = spark.createDataFrame(panda_dataframe)
    print("spark_df = ", spark_df)
    print("spark_df.show():")
    spark_df.show()

    # You may convert a Spark DataFrame into Pandas DataFrame
    pandas_df = spark_df.toPandas()
    print("pandas_df = \n", pandas_df)

    # done!
    spark.stop()
