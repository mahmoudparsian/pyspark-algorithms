#!/usr/bin/python
#-----------------------------------------------------
# 1. Create a DataFrame from a CSV File 
#    from Input as a CSV File of the 
#    following format:
#
#    <name><,><city><,><age>
#
# 2. Find average of age per city.
#------------------------------------------------------
# Input Parameters:
#   A CSV File
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
import pyspark.sql.functions as F

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
#

if __name__ == '__main__':

    if len(sys.argv) != 2:  
        print("Usage: dataframe_creation_from_csv.py <csv-file>", file=sys.stderr)
        exit(-1)

    #------------------------------------------
    # create an instance of SparkSession object
    #------------------------------------------
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_from_csv")\
        .getOrCreate()
    #
    print("spark=",  spark)

    # read name of input file
    input_path = sys.argv[1]
    print("input path : ", input_path)
    debug_file(input_path)
    

    #=====================================
    # Create a DataFrame from a given input file
    #=====================================
    
    #------------------------------
    # Define Schema for a DataFrame
    #------------------------------
    # column_names = ["name", "city", "age"]
    schema = StructType([
        StructField("name", StringType()),
        StructField("city", StringType()),
        StructField("age", DoubleType())
    ])
    
    # Spark enable us to read CSV files with or without a 
    # header; here we will read a CSV file without a header 
    # (name of columns)and create a new DataFrame
    
    # The following reads a CSV file without a 
    # header and create a new DataFrame and infers
    # a schema from the content of columns:
    df = spark\
          .read\
          .schema(schema)\
          .format("csv")\
          .option("header","false")\
          .option("inferSchema", "true")\
          .load(input_path)
    #
    print("df.count() = " , df.count())
    #
    print("df.collect() = " , df.collect())
    #
    df.show()    
    #
    df.printSchema()

    #-----------------------------
    # Method-1: find average of age per city
    #-----------------------------
    print("Method-1:")
    #average_method1 = df.groupBy("city").agg({"age": "avg"})
    average_method1 = df.groupBy("city").agg(F.avg('age').alias('average'))
    average_method1.show()


    #-----------------------------
    # Method-2: find average of age per city
    #-----------------------------
    print("Method-2:")
    df.createOrReplaceTempView("mytable")
    average_method2 = spark.sql("SELECT city, AVG(age) as average FROM mytable GROUP BY city")
    average_method2.show()
     
    # done!
    spark.stop()

