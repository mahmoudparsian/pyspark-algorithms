#-----------------------------------------------------
# This is a shell script to run dataframe_creation_cvs_with_header.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export INPUT_FILE="/pyspark_book/code/chap07/emps_with_header.txt"
export SPARK_PROG="/pyspark_book/code/chap07/dataframe_creation_cvs_with_header.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_FILE
