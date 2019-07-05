#--------------------------------------------------------
# This is a shell script to run datasource_redis_reader.py
#--------------------------------------------------------
# @author Mahmoud Parsian
#--------------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap08/datasource_redis_reader.py"
#
# define the required redis database connection parameters
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
# you may add password
#export REDIS_PASSWORD="<your-password>"
#
# define the required JAR file for redis database access
export JAR="/pyspark_book/code/jars/spark-redis-2.3.1-SNAPSHOT-jar-with-dependencies.jar"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit --jars ${JAR}  ${SPARK_PROG} ${REDIS_HOST} ${REDIS_PORT} 
