#-----------------------------------------------------
# This is a shell script to build and save an LR model 
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export TRAINING_DATA_SPAM="/pyspark_book/code/chap09/training_emails_spam.txt"
export TRAINING_DATA_NOSPAM="/pyspark_book/code/chap09/training_emails_nospam.txt"
export BUILT_MOLDEL_OUTPUT_PATH="/pyspark_book/code/chap09/model"
export SPARK_PROG="/pyspark_book/code/chap09/logistic_regression_builder.py"
#
# Make sure there are no files under output path
rm -fr ${BUILT_MOLDEL_OUTPUT_PATH}/*
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit ${SPARK_PROG} ${TRAINING_DATA_NOSPAM} ${TRAINING_DATA_SPAM} ${BUILT_MOLDEL_OUTPUT_PATH}
