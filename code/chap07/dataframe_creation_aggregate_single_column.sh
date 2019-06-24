#----------------------------------------------------------------------------
# This is a shell script to run dataframe_creation_aggregate_single_column.py
#----------------------------------------------------------------------------
# @author Mahmoud Parsian
#----------------------------------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap07/dataframe_creation_aggregate_single_column.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG
