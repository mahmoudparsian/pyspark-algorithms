#!/usr/bin/python
#-----------------------------------------------------
# Aggregate Single Column of DataFrame
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
    #    print("Usage: dataframe_creation_aggregate_single_column.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_aggregate_single_column")\
        .getOrCreate()


    # Aggregating Columns
    #
    # PySpark provides a very powerful method (`agg()`) to 
    # aggregate (such as summation or average of column values)
    # on the entire DataFrame. The `agg()` is defined as:
    #
    #  pyspark.sql.DataFrame.agg (Python method, in pyspark.sql module)
    #
    # I will demo the aggregation functions for a DataFrame.
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

    # To find sum of values for the "price" column:
    df.agg({'price': 'sum'}).show()
    # +------------------+
    # |        sum(price)|
    # +------------------+
    # |14.959999999999999|
    # +------------------+


    # To find average of values for the "price" column:
    df.agg({'price': 'mean'}).show()
    # +-----------------+
    # |       avg(price)|
    # +-----------------+
    # |1.662222222222222|
    # +-----------------+


    # To find maximum of values for the "price" column:
    df.agg({'price': 'max'}).show()
    # +----------+
    # |max(price)|
    # +----------+
    # |      2.59|
    # +----------+

    # To find minimum of values for the "price" column:
    df.agg({'price': 'min'}).show()
    # +----------+
    # |min(price)|
    # +----------+
    # |      0.45|
    # +----------+


    # Also it is possible to "GROUP BY" (similar to SQL's 
    # GROUP BY concept) a column and then apply the aggregation 
    # function. The following examples show how to perform
    # the `groupBy()` on a specific column and then apply the
    # aggregation function.
    df.groupBy("name").agg({'price': 'mean'}).show()
    # +----+------------------+
    # |name|        avg(price)|
    # +----+------------------+
    # |adam|1.8833333333333335|
    # |mary|               2.0|
    # |john|1.2433333333333334|
    # |bill|              1.79|
    # +----+------------------+

    df.groupBy("name").agg({'price': 'max'}).show()
    # +----+----------+
    # |name|max(price)|
    # +----+----------+
    # |adam|      2.44|
    # |mary|       2.0|
    # |john|      1.99|
    # |bill|      2.59|
    # +----+----------+

    df.groupBy("name").agg({'price': 'min'}).show()
    # +----+----------+
    # |name|min(price)|
    # +----+----------+
    # |adam|      1.22|
    # |mary|       2.0|
    # |john|      0.45|
    # |bill|      0.99|
    # +----+----------+


    # done!
    spark.stop()
