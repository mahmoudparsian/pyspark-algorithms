#!/bin/bash
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
SECONDS=0
/bin/date
# do some work
#
# define Spark's installed directory
export SPARK_HOME="/pyspark_book/spark-2.4.3"
#
# create one billion (key, value) pairs
python generate_key_value_pairs.py > kv.txt
#
# NOTE: define your input path
INPUT_PATH="file:///pyspark_book/code/chap02/kv.txt"
#
# define your PySpark program
PROG="/pyspark_book/code/chap02/sum_by_reducebykey.py"
#
# submit your spark application
$SPARK_HOME/bin/spark-submit $PROG $INPUT_PATH
#
#
duration=$SECONDS
echo ""
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
