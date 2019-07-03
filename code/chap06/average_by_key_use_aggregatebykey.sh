#-----------------------------------------------------
# This is a shell script for finding averages per
# key by using the aggregateByKey() transformation  
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap06/average_by_key_use_aggregatebykey.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG 

