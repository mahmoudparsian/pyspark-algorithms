#-----------------------------------------------------------
# This is a shell script to run datasource_csv_writer.py
#-----------------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap08/datasource_csv_writer.py"
export OUTPUT_CSV_FILE_PATH="/tmp/output.csv"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG ${OUTPUT_CSV_FILE_PATH}
