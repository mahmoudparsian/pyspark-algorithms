#-----------------------------------------------------
# This is a shell script to run top_N_use_takeordered.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap12/top_N_use_takeordered.py"
#
# run the PySpark program:
# find Top-3
export N = 3
$SPARK_HOME/bin/spark-submit $SPARK_PROG $N
