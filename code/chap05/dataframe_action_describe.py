#!/usr/bin/python
#-----------------------------------------------------
# Apply a describe() action to a DataFrame
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


#=========================================
def create_pair(t3):
    # t3 = (name, city, number)
    name = t3[0]
    #city = t3[1]
    number = int(t3[2])
    return (name, number)
#end-def
#==========================================

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_action_describe.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_action_describe")\
        .getOrCreate()
    #
    print("spark=",  spark)

    #========================================
    # DataFrame.describe() action
    #
    # Description:
    # Computes basic statistics for numeric and string columns.
    # This include count, mean, stddev, min, and max. If no columns 
    # are given, this function computes statistics for all numerical 
    # or string columns.
    #========================================

    pairs = [(10,"z1"), (1,"z2"), (2,"z3"), (9,"z4"), (3,"z5"), (4,"z6"), (5,"z7"), (6,"z8"), (7,"z9")]
    print("pairs = ", pairs)
    df = spark.createDataFrame(pairs, ["number", "name"])
    print("df.count(): ", df.count())
    print("df.collect(): ", df.collect())
    df.show()

    #-----------------------------------------
    # apply describe() action
    #-----------------------------------------
    df.describe(['number']).show()    
    #
    df.describe().show()    

     
    # done!
    spark.stop()

