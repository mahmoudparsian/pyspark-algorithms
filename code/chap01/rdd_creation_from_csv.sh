#-----------------------------------------------------
# This is a shell script to run rdd_creation_from_csv.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export INPUT_FILE="/pyspark_book/code/chap01/name_city_age.csv"
export SPARK_PROG="/pyspark_book/code/chap01/rdd_creation_from_csv.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit  $SPARK_PROG  $INPUT_FILE
