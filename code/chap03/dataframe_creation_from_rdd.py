#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame from an RDD
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
from pyspark.sql import Row


#

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_creation_from_rdd.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_from_rdd")\
        .getOrCreate()
    #
    print("spark=",  spark)

    #============================
    # Creating DataFrame from RDD
    #============================
    # creating a DataFrame from list of tuples:
    # Create a list of tuples. 
    # Each tuple contains name, city, and age.
    # Create a RDD from the list above.
    # Convert each tuple to a row.
    # Create a DataFrame by applying createDataFrame 
    # on RDD with the help of sqlContext.
    list_of_tuples= [('alex','Sunnyvale', 25), ('mary', 'Cupertino', 22), ('jane', 'Ames', 20), ('bob', 'Stanford', 26)]
    print("list_of_tuples = ", list_of_tuples)
    rdd = spark.sparkContext.parallelize(list_of_tuples)
    print("rdd = ", rdd)
    print("rdd.count() = ", rdd.count())
    print("rdd.collect() = ", rdd.collect())
    
    # convert rdd (as RDD[(String, String, Integer)] into RDD[Row]
    people = rdd.map(lambda x: Row(name=x[0], city=x[1], age=int(x[2])))
    print("people = ", people)
    print("people.count() = ", people.count())
    print("people.collect() = ", people.collect())

    # create a DataFrame as df
    df = spark.createDataFrame(people)
    print("df = ", df)
    print("df.count() = ", df.count())
    print("df.collect() = ", df.collect())
    df.show()
    df.printSchema()
       
    # done!
    spark.stop()

