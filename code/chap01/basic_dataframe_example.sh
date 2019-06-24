#-----------------------------------------------------
# This is a shell script to run basic_dataframe_example.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export INPUT_FILE="/pyspark_book/code/chap01/sample_people.json"
export SPARK_PROG="/pyspark_book/code/chap01/basic_dataframe_example.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit  $SPARK_PROG  $INPUT_FILE
