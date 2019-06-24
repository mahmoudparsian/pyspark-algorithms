#!/usr/bin/python
#-----------------------------------------------------
# Apply crosstab() for two columns of a DataFrame 
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
    #    print("Usage: dataframe_crosstab.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_crosstab")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    # input_path = sys.argv[1]  
    # print("input_path: {}".format(input_path))

    #===============================================
    # pyspark.sql.DataFrameStatFunctions.crosstab 
    # (Python method, in pyspark.sql module)
    #
    # crosstab(col1, col2)
    # Computes a pair-wise frequency table of the given columns. 
    # Also known as a contingency table. The number of distinct 
    # values for each column should be less than 1e4. At most 
    # 1e6 non-zero pair frequencies will be returned. The first 
    # column of each row will be the distinct values of col1 
    # and the column names will be the distinct values of col2. 
    # The name of the first column will be $col1_$col2. Pairs 
    # that have no occurrences will have zero as their counts. 
    # DataFrame.crosstab() and DataFrameStatFunctions.crosstab() 
    # are aliases.
    #
    # Parameters:	
    #    col1 - The name of the first column. 
    #           Distinct items will make the 
    #           first item of each row.
    #    col2 - The name of the second column. 
    #           Distinct items will make the 
    #           column names of the DataFrame.
    #
    # Returns: A DataFrame containing for the 
    #          contingency table.
    #===============================================
    data = [(1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2), (3, 3), (4, 4)]
    columns = ["key", "value"]
    df = spark.createDataFrame(data, columns)
    df.show()
    
    # find crosstab of two columns: "key" and "value"
    crosstab = df.crosstab("key", "value")
    crosstab.show()

    # done!
    spark.stop()
