# define Spark's installed directory
export SPARK_HOME="/pyspark_book/spark-2.4.3"
#
# define your input path
INPUT_PATH="file:///pyspark_book/code/chap04/data/sample.fasta"
#
# define your PySpark program
PROG="/pyspark_book/code/chap04/DNA-FASTA-V2/dna_base_count_ver_2.py"
#
# submit your spark application
$SPARK_HOME/bin/spark-submit $PROG $INPUT_PATH
