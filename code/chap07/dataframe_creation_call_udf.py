#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame and Call UDF 
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
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

#======================================
# Let's define a simple function, which 
# returns square of a given number.
#
def squared(n):
    return n * n
#end-def
#======================================

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_creation_call_udf.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_call_udf")\
        .getOrCreate()


    # This section contains several examples of creating a UDF in 
    # Python and registering it for use in Spark SQL. To use a UDF,
    # it is a 3-steps process:
    #
    #   STEP-1: first you define the function in your favorite 
    #           programming language such as Python or Java
    #
    #   STEP-2: then register the function as a UDF, and 
    #
    #   STEP-3: finally you may use it in Spark SQL
    #
    # UDFs can be used with DataFrames and Spark SQL, which 
    # both are covered in the following subsections.

    #-------------------------
    ## Use UDF with DataFrames
    #-------------------------
    # Let's define a simple function, which returns
    # square of a given number. (already defined)

    # let's test squared() function
    squareof3 = squared(3)
    print("squareof3=", str(squareof3))
    squareof7 = squared(7)
    print("squareof7=", str(squareof7))	

	# Next we create a DataFrame from a Python collection:
    data = [('alex', 5), ('jane', 7), ('bob', 9)]
    print("data=", data)
    # [('alex', 5), ('jane', 7), ('bob', 9)]
    df = spark.createDataFrame(data, ['name', 'age'])
    df.show()
    # +----+---+
    # |name|age|
    # +----+---+
    # |alex|  5|
    # |jane|  7|
    # | bob|  9|
    # +----+---+

    # Next we register the `squared()` function as a UDF:
    # Register your function as UDF and indicate that it 
    # returns a `LongType`
    squared_udf = udf(squared, LongType())

    # Finally, we use the registered UDF with a DataFrame:
    df.select("name", "age", squared_udf("age").alias("age_squared")).show()
    # +----+---+-----------+
    # |name|age|age_squared|
    # +----+---+-----------+
    # |alex|  5|         25|
    # |jane|  7|         49|
    # | bob|  9|         81|
    # +----+---+-----------+

    # done!
    spark.stop()
