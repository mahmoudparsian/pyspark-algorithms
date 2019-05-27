# NOTE:
#      Since statistics functions are defined in
#      Python3, we use Python3 instead of Python2
#
# define python3: find out where python3 is installed?
#$ type python3
#python3 is /usr/local/bin/python3
#$ /usr/local/bin/python3 --version
#Python 3.7.1
export PYSPARK_PYTHON=/usr/local/bin/python3
#
# define PySpark program
export PROG="/pyspark_book/code/chap01/compute_stats_with_threshold_and_filter.py"
#
# define your input path
export INPUT="/pyspark_book/code/chap01/url_frequencies.txt"
#
# define your Spark home directory
export SPARK_HOME="/pyspark_book/spark-2.4.3"
#
# define the length threshold
export THRESHOLD_RECORD_LENGTH=5
#
# define the mean threshold
export THRESHOLD_MEAN=2
#
# run the program
$SPARK_HOME/bin/spark-submit $PROG $INPUT ${THRESHOLD_RECORD_LENGTH} ${THRESHOLD_MEAN}
