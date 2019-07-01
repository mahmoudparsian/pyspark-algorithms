#-----------------------------------------------------
# This is a shell script to run dataframe_creation_from_csv_with_header.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export INPUT_FILE="/pyspark_book/code/chap03/kv_with_header.txt"
export SPARK_PROG="/pyspark_book/code/chap03/dataframe_creation_from_csv_with_header.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_FILE
