#-----------------------------------------------------------
# This is a shell script to run datasource_mongodb_reader.py
#-----------------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap08/datasource_mongodb_writer.py"
export MONGODB_COLLECTION_URI="mongodb://127.0.0.1/test.coll66"
export JAR1="/pyspark_book/code/jars/mongo-java-driver-3.8.2.jar"
export JAR2="/pyspark_book/code/jars/mongo-spark-connector_2.11-2.2.5.jar"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit --jars "${JAR1},${JAR2}" $SPARK_PROG ${MONGODB_COLLECTION_URI}
