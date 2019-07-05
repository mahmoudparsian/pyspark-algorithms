#-----------------------------------------------------
# This is a shell script to run minmax_use_mappartitions.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export INPUT_PATH="/pyspark_book/code/chap12/sample_numbers.txt"
export SPARK_PROG="/pyspark_book/code/chap12/minmax_force_empty_partitions.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_PATH
