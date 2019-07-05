#-----------------------------------------------------
# This is a shell script to run datasource_textfile_reader.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export INPUT_FILE="/pyspark_book/code/chap08/sample_numbers.txt"
export SPARK_PROG="/pyspark_book/code/chap08/datasource_textfile_reader.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_FILE
