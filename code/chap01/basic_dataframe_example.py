#!/usr/bin/python
#-----------------------------------------------------
# DataFrame Example: Demo DataFrame's 
# basic features and capabilities
#------------------------------------------------------
# Input Parameters:
#   A JSON File
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql import Row
from pyspark.sql.types import *
#
#=====================================
def debug_file(input_path):
    # Opening a file in python for reading is easy:
    f = open(input_path, 'r')

    # To get everything in the file, just use read()
    file_contents = f.read()
    
    #And to print the contents, just do:
    print ("file_contents = \n" + file_contents)

    # Don't forget to close the file when you're done.
    f.close()
#end-def
#=====================================
def create_pair(record):
    #
    tokens = record.split(",")
    #
    # name = tokens[0]
    city = tokens[1]
    age = int(tokens[2])
    #
    return (city, (age, 1))
#end-def
#=====================================


if __name__ == '__main__':

    if len(sys.argv) != 2:  
        print("Usage: basic_dataframe_example.py <json-file>", file=sys.stderr)
        exit(-1)

    #------------------------------------------
    # create an instance of SparkSession object
    #------------------------------------------
    spark = SparkSession\
        .builder\
        .appName("basic_dataframe_example")\
        .getOrCreate()
    #
    print("spark=",  spark)
    
    # read name of input file
    json_input_path = sys.argv[1]
    print("JSON input path : ", json_input_path)
    debug_file(json_input_path)

    #------------------------------------
    # Create a DataFrame from a JSON File
    #------------------------------------
    df = spark.read.json(json_input_path)
    
    #------------------------------------------------
    # Displays the content of the DataFrame to stdout
    #------------------------------------------------
    print("df.show():")
    df.show()

    #----------------------------------
    # Print the schema in a tree format
    #----------------------------------
    print("df.printSchema():")
    df.printSchema()


    # Select only the "name" column
    print('df.select("name").show():')
    df.select("name").show()


    # Select everybody, but increment the age by 1
    print("df.select(df['name'], df['age'] + 1).show():")
    df.select(df['name'], df['age'] + 1).show()


    # Select people older than 23
    print("df.filter(df['age'] > 23).show():")
    df.filter(df['age'] > 23).show()


    # Count people by age
    print('df.groupBy("age").count().show():')
    df.groupBy("age").count().show()

    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    sql_df = spark.sql("SELECT * FROM people")
    print("sql_df.show():")
    sql_df.show()


    # Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    # Global temporary view is tied to a system preserved database `global_temp`
    print('spark.sql("SELECT * FROM global_temp.people").show():')
    spark.sql("SELECT * FROM global_temp.people").show()


    # Global temporary view is cross-session
    print('spark.sql("SELECT * FROM global_temp.people").show():')
    spark.sql("SELECT * FROM global_temp.people").show()

    # done!
    spark.stop()