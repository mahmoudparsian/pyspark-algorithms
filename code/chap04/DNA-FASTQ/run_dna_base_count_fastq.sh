# define Spark's installed directory
export SPARK_HOME="/pyspark_book/spark-2.4.3"
#
# define your input path
INPUT_PATH="file:///pyspark_book/code/chap04/data/sp1.fastq"
#
# define your PySpark program
PROG="/pyspark_book/code/chap04/DNA-FASTQ/dna_base_count_fastq.py"
#
# submit your spark application
$SPARK_HOME/bin/spark-submit $PROG $INPUT_PATH
