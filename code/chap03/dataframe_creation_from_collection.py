#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame from a Collection
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
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType

from pyspark.sql import Row
from collections import OrderedDict

#===============================================
def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(sorted(d.items())))
#end-def
#================================================

#

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_creation_from_collection.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_from_collection")\
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
    df1 = spark.createDataFrame(list_of_pairs)
    print("df1 = ",  df1)
    print("df1.count = ",  df1.count())
    print("df1.collect() = ",  df1.collect())
    df1.show()
    df1.printSchema()
    
    #===================================================================
    # Create a DataFrame from a list of values with defined column names
    #===================================================================
    #list_of_pairs = [("alex", 1), ("alex", 5), ("bob", 2), ("bob", 40), ("jane", 60), ("mary", 700), ("adel", 800) ]
    #print("list_of_pairs=",  list_of_pairs)
    #
    # create a DataFrame from a collection
    # Distribute a local Python collection to form a DataFrame
    column_names = ["name", "value"]
    df2 = spark.createDataFrame(list_of_pairs, column_names)
    print("df2 = ",  df2)
    print("df2.count = ",  df2.count())
    print("df2.collect() = ",  df2.collect())
    df2.show()
    df2.printSchema()
    
    #===================================================================
    # Create a DataFrame from a list of values with explicit schema
    #===================================================================
    #list_of_pairs = [("alex", 1), ("alex", 5), ("bob", 2), ("bob", 40), ("jane", 60), ("mary", 700), ("adel", 800) ]
    #print("list_of_pairs=",  list_of_pairs)
    #
    # create a schema:
    schema = StructType([\
        StructField("name", StringType(), True),\
        StructField("age", IntegerType(), True)])
    #    
    # create a DataFrame from a collection
    # Distribute a local Python collection to form a DataFrame
    df3 = spark.createDataFrame(list_of_pairs, schema)
    print("df3 = ",  df3)
    print("df3.count = ",  df3.count())
    print("df3.collect() = ",  df3.collect())
    df3.show()
    df3.printSchema()
    
    #==========================================================================
    # Convert a standard python key value dictionary list to pyspark data frame
    #==========================================================================
    list_of_elements = [{"col1": "value11", "col2": "value12"},{"col1": "value21", "col2": "value22"},{"col1": "value31", "col2": "value32"}]
    df4 = spark.sparkContext.parallelize(list_of_elements).map(convert_to_row).toDF()
    print("df4 = ",  df4)
    print("df4.count = ",  df4.count())
    print("df4.collect() = ",  df4.collect())
    df4.show()
    df4.printSchema()
       
    # done!
    spark.stop()
