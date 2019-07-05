# define PySpark program
export PROG="/pyspark_book/code/chap12/inmapper_combiner_use_basic_mapreduce.py"
# define your input path
export INPUT="/pyspark_book/code/chap12/sample_dna_seq.txt"
# define your Spark home directory
export SPARK_HOME="/pyspark_book/spark-2.4.3"
# run the program
$SPARK_HOME/bin/spark-submit $PROG $INPUT
