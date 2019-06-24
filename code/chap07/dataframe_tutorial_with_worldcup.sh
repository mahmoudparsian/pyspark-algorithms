#-----------------------------------------------------
# This is a shell script to run dataframe_tutorial_with_worldcup.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/pyspark_book/spark-2.4.3"
#
export INPUT_PATH="/pyspark_book/code/chap07/WorldCupPlayers.csv"
# source of input data:
# https://www.kaggle.com/abecklas/fifa-world-cup/downloads/WorldCupPlayers.csv/5
#
export SPARK_PROG="/pyspark_book/code/chap07/dataframe_tutorial_with_worldcup.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit  $SPARK_PROG  $INPUT_PATH
