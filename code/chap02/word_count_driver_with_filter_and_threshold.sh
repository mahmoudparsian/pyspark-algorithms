#-----------------------------------------------------
# This is a shell script for word count in PySpark.
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export INPUT_FILE="/pyspark_book/code/chap02/sample_file_extra.txt"
export SPARK_PROG="/pyspark_book/code/chap02/word_count_driver_with_filter_and_threshold.py"
#
# define thresholds
export THRESHOLD_WORD_LENGTH=2
export THRESHOLD_FREQUENCY=1
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_FILE ${THRESHOLD_WORD_LENGTH} ${THRESHOLD_FREQUENCY}
