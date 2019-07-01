#-----------------------------------------------------
# This is a shell script to run dataframe_creation_from_collection.py
# Run this using Python3
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap03/dataframe_creation_from_collection.py"
export PYSPARK_PYTHON=python3
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG
