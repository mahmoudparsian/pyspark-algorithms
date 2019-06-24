#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame and perform Statistical Data 
# Exploration using Spark -- Five Number Summary
# Input: CSV with no header
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
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
from pyspark.sql.types import DoubleType

if __name__ == '__main__':

    if len(sys.argv) != 2:  
        print("Usage: dataframe_with_statistical_data.py <file>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_with_statistical_data")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))


    custom_schema = StructType([\
        StructField("country", StringType(), True),\
        StructField("life_exp", DoubleType(), True),\
        StructField("region", StringType(), True)\
       ])
       
    # Once your schema is ready, then you can impose
    # it to your CSV input files (with no header) to
    # create a DataFrame:
    df = spark\
       .read\
       .option("delimiter", ",")\
       .csv(input_path, schema=custom_schema)
                      
    df.show(truncate=False)
    df.printSchema()
    
    #===========================================
    # Five Number Summary
    #===========================================
    # Five number summary is one of the basic data 
    # exploration technique where we will find how 
    # values of dataset columns are distributed. In 
    # this example, we are interested to know the 
    # summary of the "life_exp" column.
    #
    # Five Number Summary Contains following information
    #    Min - Minimum value of the column
    #    First Quantile - The 25% th data
    #    Median - Middle Value
    #    Third Quartile - 75% of the value
    #    Max - maximum value
    #
    df.summary().show()
    #df.select("life_exp").summary().show()    

    #===========================================
    # Calculating Quantiles in Spark
    #===========================================
    # Quartiles can be calculated using approxQuantile 
    # method introduced in spark 2.0. This allows us to 
    # find 25%, median and 75% values like R. The name 
    # of the method suggests that we can get approximate 
    # values whenever we specify the error rate. This makes 
    # calculations much faster compared to absolute value.
    #
    # approxQuantile(col, probabilities, relativeError)
    median = df.approxQuantile("life_exp", [0.02, 0.04], 0.01) 
    print("median : ", median)

    # done!
    spark.stop()
