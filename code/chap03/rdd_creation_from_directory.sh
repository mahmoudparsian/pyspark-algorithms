#-----------------------------------------------------
# This is a shell script to run rdd_creation_from_directory.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
#
# $ cd sample_dir/
# $ ls -l
# -rw-r--r--  1 mparsian  dev  54 Nov 11 18:59 file1.txt
# -rw-r--r--  1 mparsian  dev  90 Nov 11 19:00 file2.txt
#
# $ cat file1.txt
# record 1 of file1
# record 2 of file1
# record 3 of file1
#
# $ cat file2.txt
# record 1 of file2
# record 2 of file2
# record 3 of file2
# record 4 of file2
# record 5 of file2
#
#------------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export INPUT_DIR="/pyspark_book/code/chap03/sample_dir"
export SPARK_PROG="/pyspark_book/code/chap03/rdd_creation_from_directory.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit  $SPARK_PROG  $INPUT_DIR
