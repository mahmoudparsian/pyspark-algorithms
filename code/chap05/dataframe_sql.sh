#-----------------------------------------------------
# This is a shell script to run dataframe_sql.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap05/dataframe_sql.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG
