#-----------------------------------------------------
# This is a shell script to load the built LR 
# model and to predict new emails (new_emails.txt) 
# into spam or nospam
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export NEW_EMAILS="/pyspark_book/code/chap09/new_emails.txt"
export BUILT_MOLDEL_OUTPUT_PATH="/pyspark_book/code/chap09/model"
export SPARK_PROG="/pyspark_book/code/chap09/logistic_regression_predictor.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit ${SPARK_PROG} ${BUILT_MOLDEL_OUTPUT_PATH} ${NEW_EMAILS}
