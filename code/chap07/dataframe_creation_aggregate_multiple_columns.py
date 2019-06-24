#!/usr/bin/python
#-----------------------------------------------------
# Aggragation of Multiple Columns for a DataFrame
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
from pyspark.sql.functions import expr
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField

#=========================================
# Define a udf zip_:
#
zip_ = udf(
    lambda xs, ys: list(zip(xs, ys)),
    ArrayType(StructType([StructField("_1", StringType()), StructField("_2", DoubleType())])))
#=========================================

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_creation_aggregate_multiple_columns.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_aggregate_multiple_columns")\
        .getOrCreate()


    # Aggregating Multiple Columns
    #
    # The question is how to aggregate multiple columns of 
    # a DataFrame with custom functions in Spark. For example, 
    # is there some way to specify a custom aggregation function 
    # for Spark DataFrames over multiple columns.
    #
    # Let's assume that we have a table like this of the 
    # type `(name, item, price)`:
    #
    #   =====+========+=====
    #   name | item   | price
    #   =====+========+=====
    #   mary | lemon  | 2.00
    #   adam | grape  | 1.22
    #   adam | carrot | 2.44
    #   adam | orange | 1.99
    #   john | tomato | 1.99
    #   john | carrot | 0.45
    #   john | banana | 1.29
    #   bill | apple  | 0.99
    #   bill | taco   | 2.59

    # We would like to aggregate the item and it's cost for each 
    # person into a list like this:
    # 
    #    mary | (lemon, 2.0)
    #    adam | (grape, 1.22), (carrot, 2.44), (orange, 1.99)
    #    john | (tomato, 1.99), (carrot, 0.45), (banana, 1.29)
    #    bill | (apple, 0.99), (taco, 2.59)


    # Is this possible in Spark DataFrames? The easiest 
    # way to do this as a DataFrame is to first collect 
    # two lists, and then use a UDF to zip the two lists 
    # together. 

    # First, let's create a DataFrame from sample tuples:
    df = spark.sparkContext.parallelize([\
        ("mary", "lemon", 2.00),\
        ("adam", "grape", 1.22),\
        ("adam", "carrot", 2.44),\
        ("adam", "orange", 1.99),\
        ("john", "tomato", 1.99),\
        ("john", "carrot", 0.45),\
        ("john", "banana", 1.29),\
        ("bill", "apple", 0.99),\
        ("bill", "taco", 2.59)\
        ]).toDF(["name", "food", "price"])
    #
    print("df.show():")
    df.show()
    # +----+------+-----+
    # |name|  food|price|
    # +----+------+-----+
    # |mary| lemon|  2.0|
    # |adam| grape| 1.22|
    # |adam|carrot| 2.44|
    # |adam|orange| 1.99|
    # |john|tomato| 1.99|
    # |john|carrot| 0.45|
    # |john|banana| 1.29|
    # |bill| apple| 0.99|
    # |bill|  taco| 2.59|
    # +----+------+-----+

    # Next, I show how to GROUP BY a column and aggregate 
    # the column values:
    #  <1> Group by "name"
    #  <2> Aggregate "food" and "price" columns
    #  <3> Display the aggregated values
    #     
    df.groupBy('name')\
       .agg(expr('collect_list(food) as food'), expr('collect_list(price) as price'))\
       .show(truncate=False)
    # +----+------------------------+------------------+
    # |name|food                    |price             |
    # +----+------------------------+------------------+
    # |adam|[grape, carrot, orange] |[1.22, 2.44, 1.99]|
    # |mary|[lemon]                 |[2.0]             |
    # |john|[tomato, carrot, banana]|[1.99, 0.45, 1.29]|
    # |bill|[apple, taco]           |[0.99, 2.59]      |
    # +----+------------------------+------------------+


    # Here, I show how to GROUP BY a column and aggregate the column
    # values as well as how to create a new column called "x".
    #
    # <1> Group by "name"
    # <2> Aggregate "food" and "price" columns
    # <3> Add a new column ("x") for grouped "food" column 
    # <4> Display the aggregated values   
    #
    df.groupBy('name')\
        .agg(expr('collect_list(food) as food'), expr('collect_list(price) as price'))\
        .withColumn('x', col('food'))\
        .show(truncate=False)
    # +----+------------------------+------------------+------------------------+
    # |name|food                    |price             |x                       |
    # +----+------------------------+------------------+------------------------+
    # |adam|[grape, carrot, orange] |[1.22, 2.44, 1.99]|[grape, carrot, orange] |
    # |mary|[lemon]                 |[2.0]             |[lemon]                 |
    # |john|[tomato, carrot, banana]|[1.99, 0.45, 1.29]|[tomato, carrot, banana]|
    # |bill|[apple, taco]           |[0.99, 2.59]      |[apple, taco]           |
    # +----+------------------------+------------------+------------------------+

    # Next , I show that how to perform a pairwise aggregation using 
    # the Python's `zip()` function. Before to look at the PySpark 
    # solution, let's look at how the `zip()` works in Python:
    list_a = [1, 2, 3, 4]
    list_b = [5, 6, 7, 8]
    print("list_a=", list_a)
    print("list_b=", list_b)
    print("zip(list_a, list_b)=", zip(list_a, list_b))
    print("zip(list_a, list_b)=", list(zip(list_a, list_b)))
    # [(1, 5), (2, 6), (3, 7), (4, 8)]


    # Next, I define a simple function (called `zip_()`) which 
    # zips list of strings (representing the "name" column) with 
    # list of doubles (representing the "price" column):
    #
    # Finally, I show you how to perform GROUP BY, followed by 
    # aggregation of "food" and "price" followed by zipping columns, 
    # and finally displaying the results.
    #
    # reference: https://stackoverflow.com/questions/45940270/create-a-tuple-out-of-two-columns-pyspark    
    #
    df.groupBy('name')\
         .agg(expr('collect_list(food) as food'), expr('collect_list(price) as price'))\
         .withColumn('x', zip_(col('food'), col('price')))\
         .show(truncate=False)
#         

    # +----+------------------------+------------------+------------------------------------------------+
    # |name|food                    |price             |x                                               |
    # +----+------------------------+------------------+------------------------------------------------+
    # |adam|[grape, carrot, orange] |[1.22, 2.44, 1.99]|[[grape, 1.22], [carrot, 2.44], [orange, 1.99]] |
    # |mary|[lemon]                 |[2.0]             |[[lemon, 2.0]]                                  |
    # |john|[tomato, carrot, banana]|[1.99, 0.45, 1.29]|[[tomato, 1.99], [carrot, 0.45], [banana, 1.29]]|
    # |bill|[apple, taco]           |[0.99, 2.59]      |[[apple, 0.99], [taco, 2.59]]                   |
    # +----+------------------------+------------------+------------------------------------------------+


    # One last query to show the final result:
    #   <1> Group by "name"
    #   <2> Aggregate "food" and "price" columns
    #   <3> Add a new column ("x") for zipped "food" and "price" columns 
    #   <4> Drop the "food" column
    #   <5> Drop the "price" column
    #   <6> Display the aggregated and zipped values
    df.groupBy('name')\
        .agg(expr('collect_list(food) as food'), expr('collect_list(price) as price'))\
        .withColumn('x', zip_(col('food'), col('price')))\
        .drop('food')\
        .drop('price')\
        .show(truncate=False)
    # +----+------------------------------------------------+
    # |name|x                                               |
    # +----+------------------------------------------------+
    # |adam|[[grape, 1.22], [carrot, 2.44], [orange, 1.99]] |
    # |mary|[[lemon, 2.0]]                                  |
    # |john|[[tomato, 1.99], [carrot, 0.45], [banana, 1.29]]|
    # |bill|[[apple, 0.99], [taco, 2.59]]                   |
    # +----+------------------------------------------------+


    # done!
    spark.stop()
