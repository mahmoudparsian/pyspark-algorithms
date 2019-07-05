#--------------------------------------------------------
# This is a shell script to run datasource_jdbc_writer.py
#--------------------------------------------------------
# @author Mahmoud Parsian
#--------------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
export SPARK_PROG="/pyspark_book/code/chap08/datasource_jdbc_writer.py"
#
# define the required MySQL database connection parameters
export JDBC_URL="jdbc:mysql://localhost/metadb"
export JDBC_DRIVER="com.mysql.jdbc.Driver"
export JDBC_USER="root"
export JDBC_PASSWORD="mp22_pass"
export JDBC_TARGET_TABLE_NAME="people"
#
# define the required JAR file for MySQL database access
export JAR="/pyspark_book/code/jars/mysql-connector-java-5.1.42.jar"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit --jars ${JAR}  ${SPARK_PROG} ${JDBC_URL} ${JDBC_DRIVER} ${JDBC_USER} ${JDBC_PASSWORD} ${JDBC_TARGET_TABLE_NAME}
