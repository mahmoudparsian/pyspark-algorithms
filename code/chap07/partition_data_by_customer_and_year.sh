#-----------------------------------------------------
# This is a shell script to run the following program:
#      partition_data_by_customer_and_year.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export INPUT_PATH="/pyspark_book/code/chap07/customers.txt"
export OUTPUT_PATH="/tmp/partition_demo"
export SPARK_PROG="/pyspark_book/code/chap07/partition_data_by_customer_and_year.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_PATH $OUTPUT_PATH
