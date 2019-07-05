#-----------------------------------------------------------
# This is a shell script to run datasource_elasticsearch_reader.py
#-----------------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap08/datasource_elasticsearch_reader.py"
export ELASTIC_SEARCH_HOST="localhost"
export JAR="/pyspark_book/jars/elasticsearch-hadoop-6.4.2.jar"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit --jars "${JAR}" $SPARK_PROG ${ELASTIC_SEARCH_HOST}
