#-----------------------------------------------------
# This is a shell script to run rdd_creation_from_collection.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap03/rdd_creation_from_collection.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG
