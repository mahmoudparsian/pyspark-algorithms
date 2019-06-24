#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame 
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

if __name__ == '__main__':

    if len(sys.argv) != 2:  
        print("Usage: dataframe_creation_csv_no_header.py <file>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_csv_no_header")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))

    # create a DataFrame reading a no header CSV file
    df = spark.read.csv(input_path)
    print("df.collect()=", df.collect()) 
    #[
    # Row(_c0=u'1001', _c1=u'alex', _c2=u'67000', _c3=u'SALES'), 
    # Row(_c0=u'1002', _c1=u'bob', _c2=u'24000', _c3=u'SALES'), 
    # Row(_c0=u'1003', _c1=u'boby', _c2=u'24000', _c3=u'SALES'), 
    # Row(_c0=u'1004', _c1=u'jane', _c2=u'69000', _c3=u'SOFTWARE'), 
    # Row(_c0=u'1005', _c1=u'betty', _c2=u'55000', _c3=u'SOFTWARE'), 
    # Row(_c0=u'1006', _c1=u'jeff', _c2=u'59000', _c3=u'SOFTWARE'), 
    # Row(_c0=u'1007', _c1=u'dara', _c2=u'72000', _c3=u'SOFTWARE')
    #]
    
    df.show()
    # +----+-----+-----+--------+
    # | _c0|  _c1|  _c2|     _c3|
    # +----+-----+-----+--------+
    # |1001| alex|67000|   SALES|
    # |1002|  bob|24000|   SALES|
    # |1003| boby|24000|   SALES|
    # |1004| jane|69000|SOFTWARE|
    # |1005|betty|55000|SOFTWARE|
    # |1006| jeff|59000|SOFTWARE|
    # |1007| dara|72000|SOFTWARE|
    # +----+-----+-----+--------+

    # change the default column names to your
    # desired column names by using `DataFrame.selectExpr()`:
    # rename column names
    # change default column names to your desired column names
    df2 = df.selectExpr("_c0 as id", "_c1 as name", "_c2 as salary", "_c3 as dept") 
    df2.show()
    # +----+-----+------+--------+
    # |  id| name|salary|    dept|
    # +----+-----+------+--------+
    # |1001| alex| 67000|   SALES|
    # |1002|  bob| 24000|   SALES|
    # |1003| boby| 24000|   SALES|
    # |1004| jane| 69000|SOFTWARE|
    # |1005|betty| 55000|SOFTWARE|
    # |1006| jeff| 59000|SOFTWARE|
    # |1007| dara| 72000|SOFTWARE|
    # +----+-----+------+--------+

    # Now, to see the renamed column names, we can inspect the schema:
    df2.printSchema()
    # root
    #  |-- id: string (nullable = true)
    #  |-- name: string (nullable = true)
    #  |-- salary: string (nullable = true)
    #  |-- dept: string (nullable = true)


    # To use a SQL query on a DataFrame, you have to 
    # register an alias table name (semantically, equivalent 
    # to a relational database table) for your DataFrame as:
    df2.createOrReplaceTempView("emp_table")
    print("df2=", df2)
    # DataFrame[id: string, name: string, salary: string, dept: string]

    df3 = spark.sql("SELECT * FROM emp_table WHERE id > 1002")
    df3.show()
    # +----+-----+------+--------+
    # |  id| name|salary|    dept|
    # +----+-----+------+--------+
    # |1003| boby| 24000|   SALES|
    # |1004| jane| 69000|SOFTWARE|
    # |1005|betty| 55000|SOFTWARE|
    # |1006| jeff| 59000|SOFTWARE|
    # |1007| dara| 72000|SOFTWARE|
    # +----+-----+------+--------+


    # This is another SQL query filtering based on `salary`:
    df4 = spark.sql("SELECT * FROM emp_table WHERE salary < 60000")
    df4.show()
    # +----+-----+------+--------+
    # |  id| name|salary|    dept|
    # +----+-----+------+--------+
    # |1002|  bob| 24000|   SALES|
    # |1003| boby| 24000|   SALES|
    # |1005|betty| 55000|SOFTWARE|
    # |1006| jeff| 59000|SOFTWARE|
    # +----+-----+------+--------+

    # The next SQL query projects specific columns
    # based on a `salary` filter:
    df5 = spark.sql("SELECT name, salary FROM emp_table WHERE salary > 25000")
    print("df5=", df5)
    # DataFrame[name: string, salary: string]
    df5.show()
    # +-----+------+
    # | name|salary|
    # +-----+------+
    # | alex| 67000|
    # | jane| 69000|
    # |betty| 55000|
    # | jeff| 59000|
    # | dara| 72000|
    # +-----+------+

    # Also, using SQL query, you can project, filter and sort:
    df6 = spark.sql("SELECT name, salary FROM emp_table WHERE salary > 55000 ORDER BY salary")
    print("df6=", df6)
    # DataFrame[name: string, salary: string]
    df6.show()
    # +----+------+
    # |name|salary|
    # +----+------+
    # |jeff| 59000|
    # |alex| 67000|
    # |jane| 69000|
    # |dara| 72000|
    # +----+------+

    # The next SQL query uses "GROUP BY" to group values of `dept` column:
    df7 = spark.sql("SELECT dept, count(*) as count FROM emp_table GROUP BY dept")
    print("df7=", df7)
    # DataFrame[dept: string, count: bigint]
    df7.show()
    # +--------+-----+
    # |    dept|count|
    # +--------+-----+
    # |   SALES|    3|
    # |SOFTWARE|    4|
    # +--------+-----+


    # done!
    spark.stop()
