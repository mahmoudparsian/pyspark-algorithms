#==========================================
# NOTE:
#
# In general, avoid using groupByKey(), and 
# instead use reduceByKey() or combineByKey().
# For details see: 
#   https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
#
# The groupByKey() solution is provided for educational 
# purposes.  If you need all of the values of a key for 
# some aggregation such as finding the "median" (which you
# need all of the values per key), then  the groupByKey() 
# may be used.
#==========================================
#
# define PySpark program
export PROG="/pyspark_book/code/chap12/average_monoid_use_groupbykey.py"
# define your input path
export INPUT="/pyspark_book/code/chap12/sample_input.txt"
# define your Spark home directory
export SPARK_HOME="/pyspark_book/spark-2.4.3"
# run the program
$SPARK_HOME/bin/spark-submit $PROG $INPUT
