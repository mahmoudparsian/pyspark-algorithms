from __future__ import print_function
import sys
from pyspark.sql import SparkSession
#-----------------------------------
# @author Mahmoud Parsian
#-----------------------------------
#
# Create (key, value) pair from given input record:
# record: <key><,><value>
# <1> accept a record of the form "key,value"
# <2> tokenize input record, 
#    tokens[0]: key, 
#    tokens[1]: value
# <3> return a pair of (key, value)
#
def create_pair(record):  # <1>
    tokens = record.split(',')  # <2>
    key = str(tokens[0])
    value = int(tokens[1])
    return (key, value)  # <3>
#end-def
#-----------------------------------


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ", __file__, " <input-path>", file=sys.stderr)
        exit(-1)
        
    #------------------------------------------
    # create an instance of SparkSession object
    #------------------------------------------
    spark = SparkSession\
        .builder\
        .appName("test:groupBykey()")\
        .getOrCreate()

    input_path = sys.argv[1]
    print("input path : ", input_path)

    #
    rdd = spark.sparkContext.textFile(input_path)
    print("rdd.getNumPartitions() = ", rdd.getNumPartitions()) 
    #       
    results = rdd.map(create_pair)\
        .groupByKey()\
        .mapValues(lambda values: sum(values))
    
    # display final results
    print("results = ", results.collect())

    spark.stop()
