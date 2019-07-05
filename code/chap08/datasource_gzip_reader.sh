#-----------------------------------------------------
# This is a shell script to run datasource_gzip_reader.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
FILE1=z11.file.txt.gz
FILE2=z22.file.txt.gz
export INPUT_FILE="/pyspark_book/code/chap08/sample_no_header.csv"
export SPARK_PROG="/pyspark_book/code/chap08/datasource_csv_reader_no_header.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_FILE
