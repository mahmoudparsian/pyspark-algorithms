#-----------------------------------------------------
# This is a shell script to run rdd_creation_from_dictionary.py
# Run this using Python3 by setting PYSPARK_PYTHON
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap03/rdd_creation_from_dictionary.py"
export PYSPARK_PYTHON=python3
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG
