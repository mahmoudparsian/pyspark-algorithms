#-----------------------------------------------------
# This is a shell script to run datasource_json_reader_single_line.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export INPUT_FILE="/pyspark_book/code/chap08/sample_single_line.json"
export SPARK_PROG="/pyspark_book/code/chap08/datasource_json_reader_single_line.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_FILE
