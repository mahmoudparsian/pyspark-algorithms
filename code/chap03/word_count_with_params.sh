# define Spark's installed directory
export SPARK_HOME="/pyspark_book/spark-2.4.3"
#
# define your input path
INPUT_PATH="$SPARK_HOME/licenses/LICENSE-heapq.txt"
#
# define your PySpark program
PROG=/pyspark_book/code/chap03/word_count_with_params.py
#
# submit your spark application
$SPARK_HOME/bin/spark-submit $PROG $INPUT_PATH
