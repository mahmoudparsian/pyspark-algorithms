#-----------------------------------------------------
# This is a shell script to run datasource_textfile_writer.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export OUTPUT_PATH="/tmp/zoutput"
export SPARK_PROG="/pyspark_book/code/chap08/datasource_textfile_writer.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $OUTPUT_PATH
