#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame and perform groupBy()
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
from pyspark.sql.functions import sum
from pyspark.sql.functions import lit

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_multi_dim_agg_groupby.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_multi_dim_agg_groupby")\
        .getOrCreate()


    # DataFrame.groupBy()
    # The following examples show how perform
    # a groupBy for an existing DataFrame. 


    # Let's first create a DataFrame with 3 columns:
    data = [\
              ('Ames', 2006, 100),\
              ('Ames', 2007, 200),\
              ('Ames', 2008, 300),\
              ('Sunnyvale', 2007, 10),\
              ('Sunnyvale', 2008, 20),\
              ('Sunnyvale', 2009, 30),\
              ('Stanford', 2008, 90)\
           ]
    #
    print("data = ", data)
    #
    columns = ("city", "year", "amount")
    print("columns = ", columns)
    
    # create a new DataFrame
    sales = spark.createDataFrame(data , columns)   
    print("sales DataFrame:")
    sales.show() 

    #============================================
    # DataFrame.groupBy(*cols)
    #
    # Description:
    #     Groups the DataFrame using the specified columns, 
    #     so we can run aggregation on them. See GroupedData 
    #     for all the available aggregate functions.
    #
    # groupby() is an alias for groupBy().
    #
    # Parameters:	
    #    cols - list of columns to group by. Each element should 
    #           be a column name (string) or an expression (Column).
    #============================================

    # GROUP BY (city, year)
    # subtotals by (city, year)
    groupby_city_and_year = sales\
       .groupBy('city', 'year')\
       .agg(sum('amount').alias('amount'))
    
    #
    print("# GROUP BY (city, year)")
    groupby_city_and_year.show()
    
          
    # GROUP BY (city)
    # subtotals by (city)
    groupby_city = sales\
       .groupBy('city')\
       .agg(sum('amount').alias('amount'))\
       .select('city', lit(None).alias('year'), 'amount')
    #
    print("# GROUP BY (city)")
    groupby_city.show()

    # apply UNION to groupby_city_and_year and groupby_city
    groupby_with_union = groupby_city_and_year\
      .union(groupby_city)\
      .sort('city', 'year')
#      .sort('city'.desc_nulls_last, 'year'.asc_nulls_last)
    #
    print("# groupby_with_union:")
    groupby_with_union.show()


    # done!
    spark.stop()
